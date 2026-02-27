(function() {
    'use strict';

    var config = {
        cacheTTL: 48 * 60 * 60 * 1000, // 48 часов
        tmdbBaseUrl: 'http://89.23.106.238/tmdb-proxy/',
        apiLanguage: 'ru-RU',
        maxRetries: 3,
        retryDelay: 1000,
        maxConcurrentRequests: 6,
        imageLoadTimeout: 1000,
        rescanInterval: 15000,
        debug: true
    };

    function log(...args) {
        if (config.debug) {
            console.log('[Series Label Plugin]', ...args);
        }
    }

    var pluginState = {
        observer: null,
        initialScanTimer: null,
        periodicRescanTimer: null,
        styleTagAdded: false
    };

    var cache = {
        data: {},
        processedCards: {},
        get: function(key) {
            var item = this.data[key];
            if (item && Date.now() - item.timestamp < config.cacheTTL) {
                return item.data;
            }
            delete this.data[key];
            return null;
        },
        set: function(key, data) {
            this.data[key] = { data: data, timestamp: Date.now() };
            if (Object.keys(this.data).length > 1000) {
                var oldestKey = Object.keys(this.data).sort((a, b) => this.data[a].timestamp - this.data[b].timestamp)[0];
                delete this.data[oldestKey];
            }
        },
        markCardProcessed: function(cardId) {
            this.processedCards[cardId] = Date.now();
            setTimeout(() => delete this.processedCards[cardId], 15000);
        },
        isCardProcessed: function(cardId) {
            return !!this.processedCards[cardId];
        },
        clearProcessedCards: function() {
            this.processedCards = {};
        }
    };

    var requestQueue = {
        queue: [],
        activeRequests: 0,
        add: function(task) {
            this.queue.push(task);
            this.process();
        },
        process: function() {
            if (this.activeRequests >= config.maxConcurrentRequests || !this.queue.length) return;
            this.activeRequests++;
            var task = this.queue.shift();
            task().then(() => {
                this.activeRequests--;
                this.process();
            }).catch(e => {
                log('Queue task error:', e.message);
                this.activeRequests--;
                this.process();
            });
        }
    };

    function lampaFetch(url) {
        return new Promise((resolve, reject) => {
            try {
                new Lampa.Reguest().silent(url, data => {
                    var jsonData = typeof data === 'object' ? data : JSON.parse(data);
                    resolve({ ok: true, status: 200, json: () => Promise.resolve(jsonData) });
                }, error => {
                    reject(new Error(`HTTP error ${error?.status || 500}`));
                });
            } catch (e) {
                reject(new Error(`Lampa.Reguest error: ${e.message}`));
            }
        });
    }

    function fetchWithRetry(url, retries = config.maxRetries) {
        let attempt = 1;
        function attemptFetch() {
            return lampaFetch(url).then(response => response.json()).catch(e => {
                if (attempt >= retries || e.message.includes('404') || e.message.includes('401')) {
                    throw e;
                }
                attempt++;
                log('Retrying fetch:', url, `Attempt ${attempt}/${retries}`);
                return new Promise(resolve => setTimeout(() => resolve(attemptFetch()), config.retryDelay * attempt));
            });
        }
        return attemptFetch();
    }

    function getTmdbApiKey() {
        if (typeof Lampa?.TMDB?.key === 'function') {
            var key = Lampa.TMDB.key();
            if (!key) {
                log('Error: Lampa.TMDB.key() returned empty or undefined');
            }
            return key;
        }
        log('Error: Lampa.TMDB.key() is not available');
        return null;
    }

    function parseDate(dateStr) {
        if (!dateStr) return null;
        try {
            var months = {
                'января': '01', 'февраля': '02', 'март': '03', 'апреля': '04',
                'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
            };
            var date;
            var isoMatch = dateStr.match(/(\d{4})-(\d{2})-(\d{2})/);
            if (isoMatch) {
                date = new Date(`${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}T00:00:00Z`);
            } else {
                var ruMatch = dateStr.match(/(\d+)\s+([а-я]+)\s*(\d{4})?/i);
                if (ruMatch) {
                    var day = ruMatch[1].length === 1 ? '0' + ruMatch[1] : ruMatch[1];
                    var month = months[ruMatch[2].toLowerCase()];
                    var year = ruMatch[3] || new Date().getFullYear();
                    if (month) {
                        date = new Date(`${year}-${month}-${day}T00:00:00Z`);
                    }
                }
            }
            return (!date || isNaN(date.getTime())) ? null : date;
        } catch (e) {
            return null;
        }
    }

    function calculateDaysUntil(dateStr) {
        var date = parseDate(dateStr);
        if (!date) return null;
        var today = new Date();
        today.setUTCHours(0, 0, 0, 0);
        var diffTime = date.getTime() - today.getTime();
        var diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        return diffDays >= 0 ? diffDays : null;
    }

    function calculateDaysSince(dateStr) {
        var date = parseDate(dateStr);
        if (!date) return null;
        var today = new Date();
        today.setUTCHours(0, 0, 0, 0);
        var diffTime = today.getTime() - date.getTime();
        return diffTime < 0 ? null : Math.floor(diffTime / (1000 * 60 * 60 * 24));
    }

    function createLabel(text, status) {
        var label = document.createElement('div');
        label.className = `series-label-plugin ${status}`;
        label.textContent = text;
        return label;
    }

    function searchTmdbByTitle(title, preferSeries, year) {
        if (!title || title.match(/^[0-9.]+$/)) {
            log('Skipping invalid title:', title);
            return Promise.resolve({ tmdbId: null, type: 'movie' });
        }
        var cacheKey = `search_${title}_${year || ''}_${preferSeries ? 'tv' : 'movie'}`;
        var cachedResult = cache.get(cacheKey);
        if (cachedResult) {
            log('Using cached search for:', title, cachedResult);
            return Promise.resolve(cachedResult);
        }
        var apiKey = getTmdbApiKey();
        if (!apiKey) {
            log('No TMDB API key, skipping search for:', title);
            return Promise.resolve({ tmdbId: null, type: 'movie' });
        }
        var query = encodeURIComponent(title);
        var url = `${config.tmdbBaseUrl}search/multi?api_key=${apiKey}&language=${config.apiLanguage}&query=${query}&include_adult=false${year ? `&primary_release_year=${year}` : ''}`;
        log('searchTmdbByTitle URL:', url);
        return fetchWithRetry(url).then(data => {
            log('searchTmdbByTitle Data:', data.results?.slice(0, 2) || []);
            if (data.results?.length) {
                var results = data.results.filter(item => item.media_type === 'tv' || item.media_type === 'movie');
                var match = results.find(item =>
                    (item.title?.toLowerCase() === title.toLowerCase() || item.name?.toLowerCase() === title.toLowerCase()) &&
                    (!year || (item.release_date?.startsWith(year) || item.first_air_date?.startsWith(year)))
                ) || (preferSeries && results.find(item =>
                    item.media_type === 'tv' &&
                    (item.name?.toLowerCase().includes(title.toLowerCase()) || item.original_name?.toLowerCase().includes(title.toLowerCase())) &&
                    (!year || item.first_air_date?.startsWith(year))
                )) || results.find(item =>
                    (item.name?.toLowerCase().includes(title.toLowerCase()) || item.original_name?.toLowerCase().includes(title.toLowerCase()) ||
                     item.title?.toLowerCase().includes(title.toLowerCase()) || item.original_title?.toLowerCase().includes(title.toLowerCase())) &&
                    (!year || (item.release_date?.startsWith(year) || item.first_air_date?.startsWith(year)))
                );
                if (match) {
                    var result = { tmdbId: match.id, type: match.media_type };
                    cache.set(cacheKey, result);
                    log('searchTmdbByTitle Match:', result);
                    return result;
                }
            }
            var result = { tmdbId: null, type: 'movie' };
            cache.set(cacheKey, result);
            return result;
        }).catch(e => {
            log('searchTmdbByTitle Error:', e.message);
            cache.set(cacheKey, { tmdbId: null, type: 'movie' });
            return { tmdbId: null, type: 'movie' };
        });
    }

    function getTmdbEpisodeData(tmdbId) {
        var cacheKey = `tv_${tmdbId}`;
        var cachedData = cache.get(cacheKey);
        if (cachedData) {
            log('Using cached episode data for TMDB ID:', tmdbId, cachedData);
            return Promise.resolve(cachedData);
        }
        var apiKey = getTmdbApiKey();
        if (!apiKey) {
            log('No TMDB API key, skipping episode data for TMDB ID:', tmdbId);
            return Promise.resolve({ status: 'error_fetching' });
        }
        var url = `${config.tmdbBaseUrl}tv/${tmdbId}?api_key=${apiKey}&language=${config.apiLanguage}&append_to_response=next_episode_to_air,last_episode_to_air`;
        log('getTmdbEpisodeData URL:', url);
        return fetchWithRetry(url).then(data => {
            log('getTmdbEpisodeData Data:', data);
            var result;
            if (data.status === 'Ended' || data.status === 'Canceled') {
                result = { status: 'ended' };
            } else if (data.next_episode_to_air && data.next_episode_to_air.air_date) {
                var nextAirDate = parseDate(data.next_episode_to_air.air_date);
                var daysUntilNext = calculateDaysUntil(data.next_episode_to_air.air_date);
                if (nextAirDate && daysUntilNext !== null) {
                    result = { status: 'upcoming', date: data.next_episode_to_air.air_date, parsedDate: nextAirDate, days: daysUntilNext };
                }
            } else if (data.in_production || data.status === 'Returning Series') {
                var lastAiredDateStr = data.last_episode_to_air?.air_date || data.last_air_date;
                if (lastAiredDateStr) {
                    var daysSinceLast = calculateDaysSince(lastAiredDateStr);
                    if (daysSinceLast !== null) {
                        result = { status: 'awaiting', lastEpisodeDate: lastAiredDateStr, daysSince: daysSinceLast };
                    }
                }
                if (!result) {
                    result = { status: 'awaiting_unknown' };
                }
            } else if ((data.status === 'Planned' || data.status === 'Pilot') && data.first_air_date) {
                var firstAirDate = parseDate(data.first_air_date);
                var daysUntilFirst = calculateDaysUntil(data.first_air_date);
                if (firstAirDate && daysUntilFirst !== null && daysUntilFirst > 0) {
                    result = { status: 'upcoming_first', date: data.first_air_date, parsedDate: firstAirDate, days: daysUntilFirst };
                }
            }
            if (!result) {
                result = { status: 'awaiting_unknown' };
            }
            cache.set(cacheKey, result);
            return result;
        }).catch(e => {
            log('getTmdbEpisodeData Error:', e.message);
            return { status: 'error_fetching' };
        });
    }

    function waitForImageLoad(card) {
        var img = card.querySelector('img[data-src], img[src]');
        if (!img) {
            log('No image found in card:', card.outerHTML.substring(0, 100));
            return Promise.resolve(true);
        }
        var src = img.dataset?.src || img.src;
        if (src && !src.includes('img_load.svg') && img.complete && img.naturalHeight !== 0) {
            return Promise.resolve(true);
        }
        return new Promise(resolve => {
            var timeout = setTimeout(() => resolve(true), config.imageLoadTimeout);
            var listener = () => {
                clearTimeout(timeout);
                img.removeEventListener('load', listener);
                img.removeEventListener('error', listener);
                resolve(true);
            };
            img.addEventListener('load', listener);
            img.addEventListener('error', listener);
        });
    }

    function getCardData(card) {
        var rootCard = card.closest('.card');
        if (!rootCard) {
            log('No root .card found for:', card.outerHTML.substring(0, 100));
            return null;
        }
        var tmdbId = rootCard.dataset?.id || rootCard.dataset?.tmdb_id || rootCard.dataset?.tmdb ||
                     rootCard.dataset?.series_id || rootCard.dataset?.media_id || rootCard.dataset?.content_id;
        var type = rootCard.dataset?.type;
        var titleElement = rootCard.querySelector('.card__title, .card__name');
        var title = titleElement?.textContent.trim();
        var yearElement = rootCard.querySelector('.card__year, .card__age');
        var year = yearElement?.textContent.trim().match(/\d{4}/)?.[0];
        var jsonDataAttr = rootCard.dataset?.json;
        if (jsonDataAttr) {
            try {
                var jsonData = JSON.parse(jsonDataAttr);
                tmdbId = tmdbId || jsonData.id;
                type = type || jsonData.media_type;
                title = title || jsonData.name || jsonData.title;
                year = year || (jsonData.first_air_date?.substring(0, 4) || jsonData.release_date?.substring(0, 4));
            } catch (e) {
                log('Error parsing JSON data:', e.message);
            }
        }
        if (!type) {
            type = rootCard.classList.contains('card--tv') || rootCard.classList.contains('card--serial') ||
                   rootCard.classList.contains('card--series') || rootCard.querySelector('.card__serial, .card__tv, .card__series') ? 'tv' :
                   rootCard.classList.contains('card--movie') || rootCard.querySelector('.card__movie') ? 'movie' : 'movie';
        }
        var cardId = tmdbId || (title + '_' + (year || '')) || Math.random().toString().slice(2);
        var result = { tmdbId, type, title, cardId, year };
        log('getCardData Result:', result);
        return result;
    }

    function applyLabelToCard(card, text, status) {
        var rootCard = card.closest('.card');
        if (!rootCard) {
            log('No root .card for applying label:', card.outerHTML.substring(0, 100));
            return;
        }
        log('Applying label:', { text, status, cardId: getCardData(rootCard).cardId });
        var view = rootCard.querySelector('.card__view, .card__poster, .card__img') || rootCard;
        if (getComputedStyle(view).position === 'static') {
            view.style.position = 'relative';
        }
        var existingLabel = view.querySelector('.series-label-plugin');
        if (existingLabel) {
            existingLabel.remove();
        }
        view.appendChild(createLabel(text, status));
        rootCard.classList.add('series-label-processed');
    }

    function restoreLabelFromCache(card, tmdbId) {
        var tmdbData = cache.get(`tv_${tmdbId}`);
        if (tmdbData && tmdbData.status !== 'error_fetching') {
            var labelText = null;
            var labelStatus = 'awaiting';
            if (tmdbData.status === 'ended') {
                labelText = 'Завершено';
                labelStatus = 'ended';
            } else if (tmdbData.status === 'upcoming' || tmdbData.status === 'upcoming_first') {
                if (tmdbData.days !== null) {
                    labelText = tmdbData.days === 0 ? 'Серия сегодня' :
                                tmdbData.days === 1 ? 'Серия завтра' :
                                `Серия через ${tmdbData.days} ${tmdbData.days >= 2 && tmdbData.days <= 4 ? 'дня' : 'дней'}`;
                    labelStatus = 'upcoming';
                }
            } else if (tmdbData.status === 'awaiting') {
                labelText = tmdbData.daysSince !== null ? `${tmdbData.daysSince} дн.` : 'Нет инф.';
                labelStatus = 'awaiting';
            } else if (tmdbData.status === 'awaiting_unknown') {
                labelText = 'Нет инф.';
                labelStatus = 'awaiting';
            }
            if (labelText) {
                applyLabelToCard(card, labelText, labelStatus);
                return true;
            }
        }
        return false;
    }

    function processCard(card) {
        if (!card || !card.closest('.card')) {
            log('Invalid card:', card?.outerHTML?.substring(0, 100));
            return Promise.resolve();
        }
        var cardData = getCardData(card);
        if (!cardData || !cardData.title) {
            log('No card data or title for:', card.outerHTML.substring(0, 100));
            return Promise.resolve();
        }
        if (cache.isCardProcessed(cardData.cardId) || card.closest('.card').classList.contains('series-label-processed')) {
            if (!card.querySelector('.series-label-plugin') && cardData.tmdbId && cardData.type === 'tv') {
                return Promise.resolve(restoreLabelFromCache(card, cardData.tmdbId));
            }
            log('Card already processed:', cardData.cardId);
            return Promise.resolve();
        }
        if (['Популярные фильмы', 'Популярные сериалы', 'История', 'В тренде за неделю'].includes(cardData.title)) {
            log('Skipping section title:', cardData.title);
            return Promise.resolve();
        }
        cache.markCardProcessed(cardData.cardId);
        return waitForImageLoad(card).then(() => {
            log('Processing card:', cardData);
            var preferSeries = cardData.type === 'tv';
            if (!cardData.tmdbId && cardData.title) {
                return searchTmdbByTitle(cardData.title, preferSeries, cardData.year).then(searchResult => {
                    cardData.tmdbId = searchResult.tmdbId;
                    cardData.type = cardData.type || searchResult.type;
                    return continueProcessing();
                });
            }
            return continueProcessing();

            function continueProcessing() {
                if (!cardData.tmdbId || cardData.type !== 'tv') {
                    card.closest('.card').classList.add('series-label-processed');
                    log('Not a series or no TMDB ID for:', cardData.title, cardData);
                    return Promise.resolve();
                }
                return getTmdbEpisodeData(cardData.tmdbId).then(tmdbData => {
                    if (tmdbData.status !== 'error_fetching') {
                        var labelText = null;
                        var labelStatus = 'awaiting';
                        if (tmdbData.status === 'ended') {
                            labelText = 'Завершено';
                            labelStatus = 'ended';
                        } else if (tmdbData.status === 'upcoming' || tmdbData.status === 'upcoming_first') {
                            if (tmdbData.days !== null) {
                                labelText = tmdbData.days === 0 ? 'Серия сегодня' :
                                            tmdbData.days === 1 ? 'Серия завтра' :
                                            `Серия через ${tmdbData.days} ${tmdbData.days >= 2 && tmdbData.days <= 4 ? 'дня' : 'дней'}`;
                                labelStatus = 'upcoming';
                            }
                        } else if (tmdbData.status === 'awaiting') {
                            labelText = tmdbData.daysSince !== null ? `${tmdbData.daysSince} дн.` : 'Нет инф.';
                            labelStatus = 'awaiting';
                        } else if (tmdbData.status === 'awaiting_unknown') {
                            labelText = 'Нет инф.';
                            labelStatus = 'awaiting';
                        }
                        if (labelText) {
                            applyLabelToCard(card, labelText, labelStatus);
                        }
                    }
                    card.closest('.card').classList.add('series-label-processed');
                });
            }
        });
    }

    function debouncedProcessCard(card) {
        requestQueue.add(() => processCard(card));
    }

    function findCardsContainer() {
        var selectors = [
            '.cards', '.content__cards', '.scroll__content', '.content .scroll__content',
            '.selector .scroll__content', '.content__items', '.items', '.catalog__content', '.grid__content'
        ];
        for (var i = 0; i < selectors.length; i++) {
            var container = document.querySelector(selectors[i]);
            if (container && container.querySelector('.card')) {
                return container;
            }
        }
        return document.body;
    }

    function setupObserver() {
        if (pluginState.observer) {
            pluginState.observer.disconnect();
        }
        var container = findCardsContainer();
        log('Cards container:', container.outerHTML.substring(0, 200));
        pluginState.observer = new MutationObserver(mutations => {
            log('MutationObserver triggered with', mutations.length, 'mutations');
            var cards = container.querySelectorAll('.card:not(.series-label-processed)');
            log('Cards to process:', cards.length);
            Array.from(cards).forEach((card, i) => setTimeout(() => debouncedProcessCard(card), 50 * i));
        });
        pluginState.observer.observe(container, { childList: true, subtree: true });
        pluginState.initialScanTimer = setTimeout(() => {
            var cards = container.querySelectorAll('.card:not(.series-label-processed)');
            log('Initial scan: processing', cards.length, 'cards');
            Array.from(cards).forEach((card, i) => setTimeout(() => debouncedProcessCard(card), 50 * i));
        }, 1000);
        pluginState.periodicRescanTimer = setInterval(() => {
            var cards = container.querySelectorAll('.card:not(.series-label-processed)');
            var visibleCards = Array.from(cards).filter(card => {
                var rect = card.getBoundingClientRect();
                return rect.top < window.innerHeight && rect.bottom > 0;
            });
            if (visibleCards.length > 0) {
                log('Periodic scan: processing', visibleCards.length, 'visible cards');
                visibleCards.forEach((card, i) => setTimeout(() => debouncedProcessCard(card), 50 * i));
            }
        }, config.rescanInterval);
    }

    function setupLampaListeners() {
        if (window.Lampa && Lampa.Listener) {
            Lampa.Listener.follow('render', () => {
                log('Lampa render event triggered');
                cache.clearProcessedCards();
                setupObserver();
            });
            Lampa.Listener.follow('menu', () => {
                log('Lampa menu event triggered');
                cache.clearProcessedCards();
                setupObserver();
            });
        }
    }

    function waitLampa(attempts = 20) {
        if (window.Lampa && Lampa.Reguest && Lampa.Listener && Lampa.TMDB && typeof Lampa.TMDB.key === 'function') {
            log('Lampa detected, initializing plugin');
            initPlugin();
        } else if (attempts > 0) {
            log('Lampa or dependencies not detected, retrying in 500ms');
            setTimeout(() => waitLampa(attempts - 1), 500);
        } else {
            console.error('[Series Label Plugin] Lampa, Lampa.Reguest, Lampa.Listener, or Lampa.TMDB.key not found');
        }
    }

    function initPlugin() {
        cache.clearProcessedCards();
        if (!pluginState.styleTagAdded) {
            var styleTag = document.createElement('style');
            styleTag.id = 'series-label-plugin-styles';
            styleTag.innerHTML = `
                .series-label-plugin {
                    position: absolute;
                    top: 2%;
                    right: 2%;
                    color: white;
                    padding: 0.3em 0.6em;
                    font-size: 1vmin;
                    border-radius: 0.3em;
                    z-index: 100;
                    font-weight: bold;
                    text-shadow: 0.1em 0.1em 0.2em rgba(0,0,0,0.7);
                    box-shadow: 0 0.2em 0.5em rgba(0,0,0,0.3);
                    line-height: 1.2;
                    white-space: nowrap;
                }
                .series-label-plugin.awaiting { background-color: #4caf50; }
                .series-label-plugin.ended { background-color: #f44336; }
                .series-label-plugin.upcoming { background-color: #ffeb3b; color: black; }
                @media screen and (max-width: 600px) {
                    .series-label-plugin {
                        font-size: 0.9vmin;
                        padding: 0.2em 0.5em;
                        top: 1%;
                        right: 1%;
                    }
                }
                @media screen and (min-width: 601px) and (max-width: 1024px) {
                    .series-label-plugin {
                        font-size: 1vmin;
                        padding: 0.3em 0.6em;
                    }
                }
                @media screen and (min-width: 1025px) {
                    .series-label-plugin {
                        font-size: 1.1vmin;
                        padding: 0.4em 0.7em;
                    }
                }
            `;
            document.head.appendChild(styleTag);
            pluginState.styleTagAdded = true;
        }
        setupObserver();
        setupLampaListeners();
    }

    setTimeout(waitLampa, 300);
})();
