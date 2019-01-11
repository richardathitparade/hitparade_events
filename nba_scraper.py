from scraper import HitParadeBot, HitParadeBotCommandProcessor, MessagingQueue, HitParadeOutputFactory
import time
import traceback
import os
from hitparade_pubsub import RedisCache,RedisEventSubscriber

class HitParadeNbaBotScrapeProcessor(HitParadeBotCommandProcessor):
    """
    The class that allows a HitParadeBot to Scrape a URL before or during an operation.
    IS-A HitParadeBotCommandProcessor
    """
    def __init__(self, **kwargs):
        """
        Constructor
        :param kwargs: dict with data to process
        """
        super().__init__(**kwargs)




    def run_cmd(self, **kwargs):
        """
        run_cmd called by HitParadeBot
        :param kwargs: dict with data to process
        """
        run_count = kwargs.get('run_count', 1)
        if run_count > 1:
            scraper_prerun_actions = self.bot_data.get('data_selectors', {}).get('scraper_prerun_actions', None)
            if not scraper_prerun_actions is None and len(scraper_prerun_actions) > 0:
                qsize,empty,full = MessagingQueue.q_size(id=self.bot_data['web_driver'].id, direction='meta')
                if run_count > 1:
                    self.bot_data.get('data_selectors', {})['open_url'] = True
                    if not empty and qsize > 0:
                        command, value, qsize, e, f = MessagingQueue.get_available_msg_nowait(id=self.bot_data['web_driver'].id, direction='meta', caller='HitParadeNbaBotScrapeProcessor::run_cmd')
                        while qsize > 0:
                            if command=='SUCCESS':
                                self.bot_data.get('data_selectors', {})['scraper_prerun_actions'] = \
                                    list(filter(lambda act: not act['selector'] == value['selector'], scraper_prerun_actions ))
                            command, value, qsize, e, f = MessagingQueue.get_available_msg_nowait(id=self.bot_data['web_driver'].id, direction='meta', caller='HitParadeNbaBotScrapeProcessor::run_cmd')
                            scraper_prerun_actions = self.bot_data.get('data_selectors', {}).get('scraper_prerun_actions', None)

            if len(scraper_prerun_actions) > 0:
                return None, None
        return self.scrape_data(id=self.scraper.get_id(), scraping_props=self.bot_data, caller=self.caller,  **kwargs)
        #boxscore_data = self.scrape_data(id=self.scraper.get_id(), scraping_props=self.bot_data, caller=self.caller, **kwargs)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """

        def create(self, **kwargs):
            return HitParadeNbaBotScrapeProcessor(**kwargs)

class HitParadeNBABot(HitParadeBot):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        updated_kwargs = self.get_command_to(**kwargs)
        self.output_connector = HitParadeOutputFactory.createOutputComponent(kwargs.get('output', 'HitParadeCachePublisherOuput' ), **kwargs)
        self.hit_parade_command_processor = HitParadeNbaBotScrapeProcessor(**updated_kwargs)
        self.sleep_time = 5
        self.cache = kwargs.get('cache', None)


    def reload_resources(self, id=-1):
        """
        Determines whether or not the WebScraper has exceeded memory threshold.
        If it has, it restarts the current WebScraper and continues with what ever task it was doing.
        :param id: int id of the web scraper to restart if necessary.
        :return:
        """
        if self.is_over_threshold():
            print('memory portfolio too high....quitting... removing [%s]  ' % (str(id) ))
            self.reset_bot_resources(new_scraper=self.restart_web_scraper( start_url=self.start_url, id=self.bot_data.get('web_driver', {}).id))
            return True
        return False

    def run(self):
        """
        Thread run method.
        :return:
        """
        run_count = 1
        run_kwargs = {'run_count' : run_count}
        self.is_started = True
        if not self.stopped():
            if self.is_recurring():
                exception_count = 0
                while not self.stopped() and self.run_recurring() and (not self.exit_on_exception or (self.exit_on_exception and exception_count==0)):
                    try:
                        reloaded = self.reload_resources()
                        if reloaded:
                            run_count=1
                        run_kwargs = {'run_count': run_count}
                        c, v = self.run_commands(**run_kwargs)
                        self.output_data(output=v)
                        time.sleep(self.sleep_time)
                        run_count += 1
                    except:
                        traceback.print_exc()
                        exception_count += 1
            else:
                try:
                    self.reload_resources()
                    c, v = self.run_commands()
                    self.output_data(output=v)
                except:
                    traceback.print_exc()


    def run_recurring(self):
        return True


def get_prerun_actions():
    close_dict_no_thanks = dict()
    close_dict_no_thanks['selector'] = 'button[title^="No Thanks"]'
    close_dict_no_thanks['wait_for'] = True
    close_dict1 = dict()
    close_dict1['wait_for'] = True
    close_dict1['selector'] = 'button#nba_tos_close_button'
    return [ close_dict_no_thanks,  close_dict1 ]


def get_postrun_actions():
    back_button = dict()
    back_button['selector'] = 'button[aria-label^="Go to Previous Day"]'

    return [back_button]


def get_scraper_inf(cache=None, events=[]):
    scraper_dict = dict()
    selector_dict_2 = dict()
    selector_dict_2['span.score-tile__period.multiple'] = False
    selector_dict_2['span.score-tile__period.types'] = ['text']
    selector_dict_2['is_nested'] = True
    selector_dict_2['scraper_url'] = 'http://www.nba.com'
    selector_dict_2['selectors'] = ['span.calendar_day','a.score-tile-wrapper']
    selector_dict_2['a.score-tile-wrapper.multiple'] = True
    selector_dict_2['a.score-tile-wrapper.attributes'] = ['href']
    selector_dict_2['calendar_day.multiple'] = False
    selector_dict_2['calendar_day.types'] = ['text']
    selector_dict_2['a.score-tile-wrapper.types'] = ['elements', 'attributes']
    selector_dict_2['a.score-tile-anchor.types'] = ['text', 'attributes']
    selector_dict_2['a.score-tile-anchor.attributes'] = ['href']
    selector_dict_2['a.score-tile-anchor.multiple'] = True
    selector_dict_2['a.score-tile-wrapper.selectors'] = [ 'div.team-game-score--visitor', 'div.team-game-score--home' , 'div.score__tile--nugget-show', 'a.score-tile-anchor']
    selector_dict_2['div.team-game-score--visitor.types'] = ['element']
    selector_dict_2['div.score__tile--nugget-show.types'] = ['text']
    selector_dict_2['div.score__tile--nugget-show.multiple'] = False
    selector_dict_2['div.team-game-score--home.types'] = ['element']
    selector_dict_2['div.team-game-score--visitor.selectors'] = [   'img.team-logo',
                                                          'div.score-tile__team-name',
                                                          'div.team-record' ,
                                                          'span.current_score' ]
    selector_dict_2['div.team-game-score--home.selectors'] = [      'img.team-logo',
                                                                    'div.score-tile__team-name',
                                                                    'div.team-record' ,
                                                                    'span.current_score' ]


    selector_dict_2['img.team-logo.attributes'] = ['src']
    selector_dict_2['img.team-logo.types'] = ['attributes']

    selector_dict_2['div.score-tile__team-name.types'] = ['text']
    selector_dict_2['div.team-record.types'] = ['text']
    selector_dict_2['span.current_score.types'] = ['text']

    scraper_prerun_actions = get_prerun_actions()
    scraper_postrun_actions = get_postrun_actions()
    scraper_dict['events'] = events
    scraper_dict['cache'] = cache
    #'default_parser': 'SeleniumParser',
    scraper_dict['data_selectors'] = {
                                      'events' : events,
                                      'scraper_prerun_actions' : scraper_prerun_actions,
                                      'scraper_postrun_actions' : scraper_postrun_actions,
                                      'open_url': False,
                                      'force_refresh': False,
                                      'scraper_url': 'http://www.nba.com' ,
                                      'data_selectors': [selector_dict_2],
                                      'cache': cache
                                      }
    return scraper_dict



def bot_kwargs(scraper_dict={}):
    bot_data_kwargs = dict()
    bot_data_kwargs['bot_data'] = scraper_dict
    bot_data_kwargs['scraper_url'] = scraper_dict['data_selectors']['scraper_url']
    bot_data_kwargs['memory_threshold'] = 140000000
    bot_data_kwargs['recurring'] = True
    bot_data_kwargs['number_scrapers'] = 1
    bot_data_kwargs['exit_on_exception'] = False
    bot_data_kwargs['sleep_time'] = 20
    bot_data_kwargs['command_processor'] = None
    bot_data_kwargs['cache'] =  scraper_dict['cache']
    bot_data_kwargs['events'] =  scraper_dict['events']
    return bot_data_kwargs

def bot(scraper_dict={}):
    bot_data = bot_kwargs(scraper_dict=scraper_dict)
    hit_parade_scrape_bot = HitParadeNBABot(**bot_data)
    hit_parade_scrape_bot.start()

if __name__ == '__main__':
    try:
        to = dict()
        to['host'] = 'hitparade001.et5b4b.0001.use1.cache.amazonaws.com'
        to['port'] = 6379
        to['subscribe'] = 'startScripts'
        to['publish'] = 'startScripts'
        to['command'] = u'START'
        cache = RedisCache()
        subscriber_kwargs = dict()
        subscriber_kwargs['redis'] = cache
        subscriber_kwargs['events'] =  ['events.sports.nba.games']
        subscriber = RedisEventSubscriber(**subscriber_kwargs)

        subscriber.start()
        print(os.environ['PATH'])
        scraper_dict = get_scraper_inf(cache=cache, events=['events.sports.nba.games'])
        bot(scraper_dict=scraper_dict)
    except:
        traceback.print_exc()



