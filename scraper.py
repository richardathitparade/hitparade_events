from __future__ import generators
import os
from abc import abstractmethod
import pprint
from selenium import webdriver
import traceback
from threading import Thread
from queue import Queue
from bs4 import BeautifulSoup
import threading
import json
import hashlib
import random
import psutil
import time
from registration import RegisterLeafClasses


#HitParadeOutput
class HitParadeOutputFactory:
    """
    HitParadeOutputFactory creates all HitParadeOutput components
    """
    factories = {}
    def addFactory(type_id, outputFactory):
        """
        Adds a scraper factory
        :param type_id:
        :param outputFactory:
        :return:
        """
        HitParadeOutputFactory.factories.put[type_id] = outputFactory
    addFactory = staticmethod(addFactory)

    def createOutputComponent(type_id=None,**kwargs):
        """
        creates a scraper component based upon kwargs
        :param kwargs: dict of data to create a ScraperComponent
        :return: ScraperComponent
        """
        if not type_id in HitParadeOutputFactory.factories:
            HitParadeOutputFactory.factories[type_id] = eval(type_id+'.Factory()')
        return HitParadeOutputFactory.factories[type_id].create(**kwargs)

class HitParadeOutput:
    """
    Class responsible for taking data and transmitting it to the proper medium be it a file, systemio, database or cache.
    Extend this class to add another data connector.

    A HitParadeBot object calls the output method of a HitParadeOutput object as an abstraction layer.
    """
    TYPES = ['print','cache','db']
    DEFAULT_TYPE='print'
    def __init__(self, **kwargs):
        self.type=kwargs.get('type', HitParadeOutput.DEFAULT_TYPE)

    @abstractmethod
    def output(self,output=None):
        """
        Outputs the data passed in
        :param output: Data passed in
        :return: nothing
        """
        pass

class HitParadeCachePublisherOuput(HitParadeOutput):
    """
    The default HitParadeOutput object.
    This pretty prints data to the commandline unless the data is none.  Then it just prints 'None'
    """
    def __init__(self,**kwargs):
        """
        Pull data for an output from kwargs
        :param kwargs: Data Dictionary passed to HitParadeOutput
        """
        super().__init__(**kwargs)
        self.cache = kwargs.get('cache', None)
        self.events = kwargs.get('events', [])

    def output(self,output=None):
        """
        overridden output command for the HitParadeOutput object.
        :param output: dict or str object to process.
        :return: none
        """
        if not output is None:
            for event_string in self.events:
                if not isinstance(output, str):
                    self.cache.publish_data(event_string, json.dumps(output))
                elif isinstance(output, str):
                    self.cache.publish_data(event_string, output)


    class Factory:
        """
        Factory class used by the HitParadeOutputFactory
        """
        def create(self, **kwargs): return HitParadeCachePublisherOuput(**kwargs)

class HitParadeDefaultOuput(HitParadeOutput):
    """
    The default HitParadeOutput object.
    This pretty prints data to the commandline unless the data is none.  Then it just prints 'None'
    """
    def __init__(self,**kwargs):
        """
        Pull data for an output from kwargs
        :param kwargs: Data Dictionary passed to HitParadeOutput
        """
        super().__init__(**kwargs)

    def output(self,output=None):
        """
        overridden output command for the HitParadeOutput object.
        :param output: dict or str object to process.
        :return: none
        """
        if output is None:
            print('None')
        else:
            PrintUtil.pprj(output)

    class Factory:
        """
        Factory class used by the HitParadeOutputFactory
        """
        def create(self, **kwargs): return HitParadeDefaultOuput(**kwargs)

#HitParadeBotCommandProcessor
class HitParadeBotCommandProcessorFactory:
    """
    HitParadeOutputFactory creates all HitParadeOutput components
    """
    factories = {}
    def addFactory(type_id, botCommandProcessorFactory):
        """
        Adds a scraper factory
        :param type_id:
        :param outputFactory:
        :return:
        """
        HitParadeBotCommandProcessorFactory.factories.put[type_id] = botCommandProcessorFactory
    addFactory = staticmethod(addFactory)

    def createBotCommandProcessor(type_id=None,**kwargs):
        """
        creates a scraper component based upon kwargs
        :param kwargs: dict of data to create a ScraperComponent
        :return: ScraperComponent
        """
        if not type_id in HitParadeBotCommandProcessorFactory.factories:
            HitParadeBotCommandProcessorFactory.factories[type_id] = eval(type_id+'.Factory()')
        return HitParadeBotCommandProcessorFactory.factories[type_id].create(**kwargs)

class HitParadeBotCommandProcessor:
    __metaclass__ = RegisterLeafClasses
    """
    Class deinfed to send commands to a WebScraper to perform an action or scrape and retrieve data.
    Default commands are defined within the class.
    """
    def __init__(self, **kwargs):
        """
        Constructor with kwargs as the data
        :param kwargs: Data Dictonary to pull data from.
        """
        self.bot_data = kwargs.get('bot_data', {})
        self.bot  = kwargs.get('bot', None)
        self.cache = kwargs.get('cache', None)
        self.command_processors = kwargs.get('command_processors', [])
        self.scraper = kwargs.get('scraper', None) if not kwargs.get('scraper', None) is None else self.bot.scrapers[0]
        self.caller = 'HitParadeBotCommandProcessor-Default::' if self.bot is None or self.bot.get_name() is None else self.bot.get_name()

    def run_command(self,id=-1, command=None, d=None, caller=None):
        """
        Sends a command to the proper MessagingQueue with the proper data object and command string.
        After a successful send if the object is not processing a QUIT command, the HitParadeBotCommandProcessor then listens to an incoming queue
        where the WebScraper sends a reply as to the success or failure of the command.

        The QUIT command is simply sent and all queues associated with the id are destroyed.
        :param id: int ID of the webscraper.
        :param command: str Command to perform.
        :param d: dict data dictionary of the command request
        :param caller: Thread calling this action usually  the main thread.  Used for logging purposes.
        :return:
        """
        MessagingQueue.send_msg(id=id, direction='in', cmd=command, d=d, caller=caller)
        command = command
        val = None
        if not command == 'QUIT':
            command, val = MessagingQueue.wait_for_msg(direction='out', id=id, caller=caller)
        print('command %s  ' % command)
        return command, val

    def login(self,id=-1, selectors=None, login_page_selectors=None, login_button_selector=None, caller=None, **kwargs):
        """
        Atomic Scraping Method
        Sends the login command to a WebScraper object.

        :param id: int id of the WebScraper to send to.
        :param selectors: list[str] list of selectors for username/password
        :param login_page_selectors: list[str] list of selectors to identify if you are on the login page.
        :param login_button_selector: str selector indicating the button to click to login.
        :param caller: str of the calling Thread - usually the Main thread.
        :return:  [2 values] command, value
        """
        message_dict = dict()
        message_dict['selectors'] = selectors
        message_dict['login_page_selectors'] = login_page_selectors
        message_dict['login_button_selector'] = login_button_selector
        message_dict['command'] = 'LOGIN'
        message_dict['type_id'] = 'ScraperLogin'
        message_dict.update(kwargs)
        return self.run_command(id=id, command='LOGIN', d=message_dict, caller=caller)

    def open_url(self,id=-1, url=None, caller=None, **kwargs):
        message_dict = dict()
        message_dict['command'] = 'OPEN'
        message_dict['type_id'] = 'ScraperAction'
        message_dict['scraper_url'] = url
        message_dict.update(kwargs)
        return self.run_command(id=id, command='OPEN', d=message_dict, caller=caller)

    def scrape_data(self,id=-1, scraping_props=None, caller=None, **kwargs):
        """
        Atomic Scraping Method
        Command that tells a WebScraper to Scrape Data from a URL.
        :param id: int id of the WebScraper to command.
        :param scraping_props: dict all properties to scrape.
        :param caller: str usually main thread
        :return:   [2 values] command, value
        """
        message_dict = dict()
        message_dict['scraper_logins'] = scraping_props.get( 'scraper_logins', [] )
        message_dict['data_selectors'] = scraping_props.get( 'data_selectors', None )
        message_dict['web_driver'] = scraping_props.get( 'web_driver', None )
        message_dict['command'] = 'SCRAPE'
        message_dict['type_id'] = 'Scraper'
        message_dict.update(kwargs)
        return self.run_command(id=id, command='SCRAPE', d=message_dict, caller=caller)

    def quit(self,id=-1, caller=None, **kwargs):
        """
        Atomic Scraping Method
        Command that tells the WebScraper to quit and shutdown.
        This command will force MessagingQueues associated with the id to be destroyed.

        :param id: int id of the WebScraper
        :param caller: str caller of the command usually Main Thread
        :return:   [2 values] command, value
        """
        message_dict = dict()
        message_dict['command'] = 'QUIT'
        message_dict['type_id'] = 'ScraperAction'
        message_dict.update(kwargs)
        return self.run_command(id=id, command='QUIT', d=message_dict, caller=caller)

    @abstractmethod
    def run_cmd(self, **kwargs):
        """
        Runs any command sent to it.
        Command Processor command to run.
        May combine the default commands or create a new command in this method.
        This is the method that the HitParadeBot object calls.

        :param kwargs: dict of data to use if necessary.
        :return:   [2 values] command, value
        """
        pass

class HitParadeBotQuitCommandProcessor(HitParadeBotCommandProcessor):
    """
    This class allows a HitParadeBot to call the QuitCommand.
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
        return self.quit(id=self.scraper.get_id(), caller=self.caller)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """
        def create(self, **kwargs): return HitParadeBotQuitCommandProcessor(**kwargs)

class HitParadeBotLoginProcessor(HitParadeBotCommandProcessor):
    """
    The class that allows a HitParadeBot to Login if necessary that is if the login page is showing at any time during the scrape.
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
        return self.login(id=self.scraper.get_id(), selectors=kwargs.get('selectors', None), login_page_selectors=kwargs.get('login_page_selectors', None), login_button_selector=kwargs.get('login_button_selector', None), caller=self.caller)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """
        def create(self, **kwargs): return HitParadeBotLoginProcessor(**kwargs)

class HitParadeBotOpenUrlProcessor(HitParadeBotCommandProcessor):
    """
    The class that allows a HitParadeBot to Open a Url before or during an operation.
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
        return self.open_url(id=self.scraper.get_id(), url=kwargs.get('url', None), caller=self.caller)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """

        def create(self, **kwargs): return HitParadeBotOpenUrlProcessor(**kwargs)

class HitParadeBotScrapeProcessor(HitParadeBotCommandProcessor):
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
        return self.scrape_data(id=self.scraper.get_id(), scraping_props=self.bot_data, caller=self.caller)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """

        def create(self, **kwargs):
            return HitParadeBotScrapeProcessor(**kwargs)

#HitParadeBot
#Each New Bot needs to extend this class and impliment abstract methods.
class HitParadeBot(Thread):
    """
    The HitParadeBot combines one or many custom commands to perform an action on the web and return a result.
    This will start with opening a URL.
    And combinations of:
    login
    scraping data
    clicking buttons and inputting data
    at the end of the operations, data and a command will be returned.

    QUIT command will be issued.

    A bot can be a one time bot that scrapes a page and ends for instance.
    A bot can be a recurring bot that runs every 40 seconds and scrapes the same page.

    As data is retrieved, data is sent to a HitParadeOutput object.
    If you wish to change where data is sent, please create a new HitParadeOutput
    object or use one of the default objects provided.
    """
    #Default number of retries for a web scraper
    DEFAULT_RETRY = 3
    #Deafult timeout for a web scraper
    DEFAULT_TIMEOUT = 5
    #Default type of web scraper
    DEFAULT_SCRAPER_TYPE = 'FirefoxWebScraper'
    #Default maximum memory threshold for this program before the web scraper is destroyed and restarted for memory efficiency.
    DEFAULT_MEMORY_THRESHOLD = 90000000
    #default scraper count.
    DEFAULT_SCRAPER_COUNT = 1
    #default sleep time for when a bot is recurring, how many seconds should the bot sleep between operations
    DEFAULT_SLEEP_TIME = 10
    #default exit_on_exception
    DEFAULT_EXIT_ON_EXCEPTION = False
    def __init__(self, **kwargs):
        """
        Constructor HitParadeBot which is a Thread
        IS-A Thread
        :param kwargs: The following properties are pulled from kwargs
        scrapers - [] list of active scrapers  default is []
        bot_data - dict of data for the bot to use. default is {}
        scraper_type - FirefoxWebScraper or ChromeWebScraper are the options.  default FireFoxWebScraper
        start_url - url to start with. Default is None
        memory_threshold - memory threshold to which the web scraper will be rebooted if it takes up too much memory.
        recurring - bool True if the Bot will be running in a loop for a certain amount of time, false otherwise.
        headless - bool True if the WebScraper should be headless and False if the WebScraper can be show.  Default is False.
        timeout - int timeout in seconds of a url.  Default  HitParadeBot.DEFAULT_TIMEOUT
        retry - int number of time s to retry a url or task.  Default  HitParadeBot.DEFAULT_RETRY
        output_connector - HitParadeOUtput object to output retrieved or scraped data from the bot. Default HitParadeDefaultOutput which pretty prints to the system/io
        number_scrapers - int number of WebScrapers to launch. Default HitParadeBot.DEFAULT_SCRAPER_COUNT
        exit_on_exception - bool True if you want the bot to exit on the first exception and False if you want the bot to continue despite the exception. Default is  HitParadeBot.DEFAULT_EXIT_ON_EXCEPTION
        sleep_time - int number of seconds to sleep on a recurring bot.  default is HitParadeBot.DEFAULT_SLEEP_TIME
        hit_parade_command_processor - HitParadeCommandProcessor to run.  Default is None
        """
        Thread.__init__(self)
        self.scrapers = kwargs.get('scrapers', [])
        self.bot_data = kwargs.get('bot_data', {})
        self.scraper_type = kwargs.get('scraper_type', HitParadeBot.DEFAULT_SCRAPER_TYPE)
        self.start_url =  kwargs.get('start_url', None)
        self.process = psutil.Process(os.getpid())
        self.memory_threshold = kwargs.get('memory_threshold', HitParadeBot.DEFAULT_MEMORY_THRESHOLD)
        self.recurring = kwargs.get('recurring', True)
        self.is_init = True
        self.is_started = False
        self.headless = kwargs.get('headless', False)
        self.cache = kwargs.get('cache', None)
        self.timeout = kwargs.get('timeout', HitParadeBot.DEFAULT_TIMEOUT)
        self.retry = kwargs.get('retry', HitParadeBot.DEFAULT_RETRY)
        self.id = MessagingQueue.unique_id()
        self.output_connector = HitParadeOutputFactory.createOutputComponent(kwargs.get('output', 'HitParadeDefaultOuput' ), **kwargs)
        self.number_scrapers = kwargs.get('number_scrapers', HitParadeBot.DEFAULT_SCRAPER_COUNT)
        self.scraper_ids_created, self.scrapers_created = self.start_scrapers()
        self._stop_event = threading.Event()
        self.exit_on_exception = kwargs.get('exit_on_exception', HitParadeBot.DEFAULT_EXIT_ON_EXCEPTION)
        self.sleep_time = kwargs.get('sleep_time', HitParadeBot.DEFAULT_SLEEP_TIME)
        self.hit_parade_command_processor = kwargs.get('command_processor', None)
        self.bot_name = 'HitParadeBot[' + str(self.id) + ']' if self.bot_data.get('bot_name', None) is None else self.bot_data.get('bot_name', None)

    def get_command_to(self, **kwargs):
        """
        Creates a dict as a transfer object to pass to the command processor.
        :param kwargs: dict in the event you wish to override this method.
        :return: dict as a transfer object to be used when creating a HitParadeCommandProcessor
        """
        command_processor_data = dict()
        command_processor_data['bot_data'] = self.bot_data
        command_processor_data['cache'] = self.cache
        command_processor_data['bot'] = self
        command_processor_data['scraper'] = kwargs.get('scraper', None) if not kwargs.get('scraper', None) is None else self.scrapers[0]
        command_processor_data['command_processors'] = None
        return command_processor_data

    def start_scrapers(self):
        """
        Start the WebScrapers if necessary.
        :return:
        """
        scrapers_created = 0
        scraper_ids_created = []
        if len(self.scrapers) < self.number_scrapers:
            number_of_scrapers_to_create = self.number_scrapers - len(self.scrapers)
            for i in range(number_of_scrapers_to_create):
                new_scraper = self.start_web_scraper( start_url=None  )
                if not new_scraper is None:
                    scrapers_created += 1
                    scraper_ids_created.append(new_scraper.get_id())
                    self.scrapers.append(new_scraper)
        return scraper_ids_created, scrapers_created

    def stop(self):
        """
        Sends a stop signal to the HitParadeBot
        :return:
        """
        self._stop_event.set()

    def stopped(self):
        """
        Tells the uer if the HitParadeBot is stopped.
        :return:  True if stopped and False if not Stopped.
        """
        return self._stop_event.is_set()

    def get_id(self):
        """
        gets the id of the HitParadeBot
        :return:  int id
        """
        return self.id

    def output_data(self, output=None):
        """
        Sends the output to the hitparade_command_processor
        :param output: dict or string to output
        :return:
        """
        self.output_connector.output(output)

    @abstractmethod
    def run_recurring(self):
        pass

    def run_commands(self, **kwargs):
        """
        Runs the hit_parade_command_processor command
        :param kwargs:
        :return:
        """
        return self.hit_parade_command_processor.run_cmd( **kwargs )

    def run(self):
        """
        Thread run method.
        :return:
        """
        self.is_started = True
        if not self.stopped():
            if self.is_recurring():
                exception_count = 0
                while not self.stopped() and self.run_recurring() and (not self.exit_on_exception or (self.exit_on_exception and exception_count==0)):
                    try:
                        self.reload_resources()
                        c, v = self.run_commands()
                        self.output_data(output=v)
                        time.sleep(self.sleep_time)
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

    def reset_bot_resources(self, new_scraper=None):
        """
        Private helper method when restarting a WebScraper
        :param new_scraper:
        :return:
        """
        if not new_scraper is None:
            self.bot_data['web_driver'] = new_scraper
            self.bot_data['driver'] = new_scraper.driver
            return True
        return False

    def reload_resources(self, id=-1):
        """
        Determines whether or not the WebScraper has exceeded memory threshold.
        If it has, it restarts the current WebScraper and continues with what ever task it was doing.
        :param id: int id of the web scraper to restart if necessary.
        :return:
        """
        if self.is_over_threshold():
            print('memory portfolio too high....quitting... removing [%s]  ' % (str(id) ))
            self.reset_bot_resources(new_scraper=self.restart_web_scraper( start_url=self.start_url, id=id))

    def get_memory_used(self):
        """
        Returns size of memory profile of this program
        :return: int size of the memory profile.
        """
        return int(self.process.memory_info().rss)

    def is_over_threshold(self):
        """
        Determines whether the current memory profile is larger than the memory threshold of the program.
        :return: True if memory used is > memory_threshold and False otherwise.
        """
        return self.get_memory_used() > self.memory_threshold

    def is_started(self):
        """
        Returns true if the bot is initialized and the webscrapers have been started.
        :return: True if bot is initialzed and web scrapers have been started.
        """
        return self.is_started

    def is_initialized(self):
        """

        :return:
        """
        return self.is_init

    def is_recurring(self):
        """
        identifies if this bot is a recurring bot or a one time bot.
        :return: True if is recurring and False if it is a one time bot.
        """
        return self.recurring

    def get_name(self):
        """
        Returns the name of the bot for logging.
        :return: str name of the bot.  Could be string version of the id or something a little more elaborate.
        """
        return self.bot_name

    def start_web_scraper( self, start_url=None ):
        """
        Starts web scraper
        Started as daemon thread.
        added to the active scrapers.
        :param start_url: url to open
        :return: WebScraper that was started
        """
        unique_id = MessagingQueue.unique_id()
        print('thread id is %s ' % unique_id)
        s = WebScraperFactory.createScraper( self.scraper_type, headless=self.headless, timeout=self.timeout, start_url=start_url, id=unique_id)
        s.setDaemon(True)
        self.scrapers.append(s)
        self.bot_data['driver'] = s.driver
        self.bot_data['web_driver'] = s
        s.start()
        return s

    def find_scraper(self,id=-1):
        """
        Returns the scraper being searched for based upon the id of the scraper.
        :param id: int id of the scraper to search for.
        :return: WebScraper that is being searched for or None if the id is not found.
        """
        if id == -1 and len(self.scrapers) == 1:
            return self.scrapers[0]
        else:
            return list(filter(lambda s: s.get_id() == id, self.scrapers))
            # if len(thread_to_find) == 1:
            #     return thread_to_find[0]
            # else:
            #     return None

    def restart_web_scraper( self, id=-1, start_url=None ):
        """
        restarts the web scraper based upon the id.
        sets the start_url to open upon the opening the new web scraper.
        :param id: int id of web scraper to remove.  if it is < 0, then we assume the scrapers
        :param start_url: str url to open
        :return: webscraper that has been newly created.
        """
        scraper_to_stop = self.find_scraper(id=id)
        if not scraper_to_stop is None:

            for s in scraper_to_stop:
                try:
                    print('Stopping thread %s ' % str(s.get_id()))
                    s.stop()
                    s.quit()
                except:
                    traceback.print_exc()

        else:
            print('Not Stopping thread %s.  Not found in list' % str(id))
        return self.start_web_scraper( start_url=start_url  )

#Thread Messaging Utils
class MessagingQueue:
    """
    Messaging between HitParadeBot and WebScraper
    This is how the Thread based classes communicate.
    Each web scraper has an id and that id is the index of all communication with a web scraper.
    direction in denotes communication via input queue
    directiion out denotes communication on output queue
    The main thread listens to the output queue
    The web scraper listens to the input queue

    To send a message to a web scraper with id 44.  send( id=44, direction='in', dict, 'SCRAPE' )
    To send a messages from a web scraper with 44 to the main thread.  Have the WebScraper call send( id=44, direction='out', dict, 'SCRAPE' )

    This class is a singleton.

    """
    class __MessagingQueue:
        """
        Privatized MessagingQueue used in the singleton.
        """
        def __init__(self):
            """
            Constructor
            """
            self.input_queues = dict()
            self.output_queues = dict()
            self.meta_queues = dict()

        def get_id(self):
            """
            Returns a unique id for a thread to use that has already not been used.
            :return:
            """
            random_int = random.randint(1, 10000)
            while random_int in self.input_queues:
                random_int = random.randint(1, 10000)
            #Set up the queues
            self.input_queues[random_int] = Queue()
            self.meta_queues[random_int] = Queue()
            self.output_queues[random_int] = Queue()
            return random_int

        def send(self, id=-1, direction='in', cmd=None, d=None,nowait=False):
            """
            Sends a message
            :param id:  int id of the webscraper
            :param direction: str 'in' for input queue, 'out' for output queue.
            :param cmd: str 'SCRAPE','OPEN','QUIT','LOGIN'
            :param d: dict
            :return: bool True if the message was sent and False if the message was NOT sent.
            """
            try:
                queue_dict = self.input_queues if direction == 'in' else self.output_queues if direction =='out' else self.meta_queues
                queue = queue_dict.get(id, None)
                if not queue is None:
                    if nowait:
                        queue.put_nowait((cmd, d))
                    else:
                        queue.put((cmd, d))
                    return True
                else:
                    return False
            except:
                traceback.print_exc()
                return False

        def qsize(self, id=-1,direction='in'):
            """
            Gets the metrics of the queue in question if a consumer wants to make a quick get.
            :param direction: str 'in' , 'out', 'meta'
            :return: int,bool,bool   QueueSize, Empty, Full
            """
            try:
                queue_dict = self.input_queues if direction == 'in' else self.output_queues if direction == 'out' else self.meta_queues
                queue = queue_dict.get(id, None)
                if not queue is None:
                    return queue.qsize(), queue.empty(), queue.full()
                else:
                    return -1, False, False
            except:
                traceback.print_exc()

        def send_nowait(self, id=-1, direction='in', cmd=None, d=None):
            return self.send(id=id,direction=direction,cmd=cmd,d=d,nowait=True)

        def quit(self, id=-1):
            """
            Quit command cleans the resources of the id of the specified queue.
            :param id: int id of the webscraper
            :return:
            """
            input_queue = self.input_queues.get(id, None)
            output_queue  = self.output_queues.get(id, None)
            meta_queue = self.meta_queues.get(id, None)
            if not input_queue is None:
                del self.input_queues[id]
            if not output_queue is None:
                del self.output_queues[id]
            if not meta_queue is None:
                del self.meta_queues[id]

    def __init__(self):
        """
        Constructor
        """
        MessagingQueue.instance = MessagingQueue.__MessagingQueue()

    instance = __MessagingQueue()

    def q_size(id=-1,direction='in'):
        """
        static method checks the size of a queue.
        :param direction: str 'in','out','meta'
        :return: int,bool,bool queue size, empty, full
        """
        return MessagingQueue.instance.qsize(id=id,direction=direction)

    def wait_for_msg(id=-1,direction='in', caller=None):
        """
        Statig method:  Wait for the proper queue to have a message and retrieve the message.
        :param direction: str 'in','out','meta'
        :param caller: str 'caller'
        :return: str,str  'key','value'
        """
        queue_dict = MessagingQueue.instance.input_queues if direction == 'in' else MessagingQueue.instance.output_queues if direction=='out' else MessagingQueue.instance.meta_queues
        queue = queue_dict.get(id, None)
        if not queue is None:
            dir = '--->'
            if direction == 'out':
                dir = '<---'
            print('{%s} %s ...waiting from %s' % (id, dir, caller))
            o,v = queue.get()
            return o,v

    def get_available_msg_nowait(id=-1,direction='in', caller=None):
        queue_dict = MessagingQueue.instance.input_queues if direction == 'in' else MessagingQueue.instance.output_queues if direction=='out' else MessagingQueue.instance.meta_queues
        queue = queue_dict.get(id, None)
        qsize, e, f = MessagingQueue.instance.qsize(id=id, direction=direction)
        if qsize > 0 and not e and not queue is None:
            dir = '--->'
            if direction == 'out':
                dir = '<---'
            print('GET_NOWAIT::{%s} %s ...waiting from %s' % (id, dir, caller))
            command, obj = queue.get(False)
            qsize, e, f = MessagingQueue.instance.qsize(id=id, direction=direction)
            return command, obj, qsize, e, f
        else:
            return None,None,qsize,e,f

    def unique_id():
        """
        generates a unique id for use within a scraper or bot.
        :return: int
        """
        return MessagingQueue.instance.get_id()

    def quit(id=-1, caller=None):
        """
        quit the resources of the sepcified queue.
        :param id: int id of the webscraper to quit.
        :param caller: str caler of the webscraper
        :return: bool True if quit and False if failed.
        """
        print('quit called from %s for %s ' % (caller, str(id) ))
        return MessagingQueue.instance.quit(id=id)

    def send_msg(id=-1,direction='in', cmd=None, d=None, caller=None):
        """
        static method send_message  sends a message and waits if necessary if Queue is full.
        :param id: int id
        :param direction: str 'in','out','meta'
        :param cmd: str command
        :param d: dict data
        :param caller: str caller
        :return:  bool True if sent False otherwise
        """
        dir = '--->'
        if direction == 'out':
            dir = '<---'
        print('{%s} %s [%s] from %s' % (id, dir,cmd, caller))
        return MessagingQueue.instance.send(id=id, direction=direction, cmd=cmd, d=d)

    def send_msg_nowait(id=-1,direction='in', cmd=None, d=None, caller=None):
        """
        sends a message without waiting.
        :param direction: str 'in','out','meta'
        :param cmd: str command
        :param d: dict data object
        :param caller: str 'method caller'
        :return: bool True if sent False otherwise
        """
        dir = '--->'
        if direction == 'out':
            dir = '<---'
        print('{%s} %s [%s] from %s' % (id, dir,cmd, caller))
        return MessagingQueue.instance.send(id=id, direction=direction, cmd=cmd, d=d, nowait=True)

    unique_id = staticmethod(unique_id)
    send_msg = staticmethod(send_msg)
    quit = staticmethod(quit)

#Logging Utils
class PrintUtil:
    """
    PrintUtil pretty print dict or str
    """
    class __PrintUtil:
        """
        Private PrintUtil class for the singleton.
        """
        def __init__(self, indent=4):
            """
            Constructor
            :param indent: int tabs to indent.
            """
            self.pp = pprint.PrettyPrinter(indent=indent)

        def pretty_print(self, obj):
            """
            Pretty Print to use
            :param obj: dict or str object to pretty print
            :return:
            """
            try:
                if not obj is None:
                    self.pp.pprint(obj)
            except:
                traceback.print_exc()

        def pretty_print_json(self, obj):
            """
            Pretty Print to use
            :param obj: dict or str object to pretty print
            :return:
            """
            try:
                if not obj is None and isinstance(obj, dict):
                    dict_str = json.dumps(obj)
                    sha_string = hashlib.sha256(dict_str.encode('utf-8')).hexdigest()
                    print('hash is %s and has changed or is first pull' % sha_string)
                    if not obj is None:
                        self.pp.pprint(obj)
            except:
                traceback.print_exc()

    def __init__(self, indent=4):
        """
        Constructor of PrintUtil initializes the instance
        :param indent:
        """
        if not PrintUtil.instance:
            PrintUtil.instance = PrintUtil.__PrintUtil(indent=indent)
        else:
            PrintUtil.instance.pp = pprint.PrettyPrinter(indent=indent)
    instance = __PrintUtil()

    def ppr(obj):
        """
        static method to call pretty_print
        :param obj: str or dict to string
        :return:
        """
        if isinstance(obj,str):
            PrintUtil.instance.pretty_print(obj)
        else:
            PrintUtil.instance.pretty_print_json(obj)

    def pprj(obj):
        """
        static method to call pretty_print
        :param obj: str or dict to string
        :return:
        """
        if isinstance(obj,str):
            PrintUtil.instance.prety_print(obj)
        else:
            PrintUtil.instance.pretty_print_json(obj)
    ppr = staticmethod(ppr)

#WebScrapers and factories
class WebScraperFactory:
    """
    WebScraperFactory creates the types of WebScrapers
    """
    factories = {}
    def addFactory(type_id, scraperFactory):
        """
        adds a factory
        :param type_id:
        :param scraperFactory:
        :return:
        """
        WebScraperFactory.factories.put[type_id] = scraperFactory
    addFactory = staticmethod(addFactory)

    def createScraper(type_id, **kwargs):
        """
        Creates a new scraper.
        :param kwargs:  dict of data to use.
        :return: WebScraper
        """
        if not type_id in WebScraperFactory.factories:
            WebScraperFactory.factories[type_id] = eval(type_id + '.Factory()')
        return WebScraperFactory.factories[type_id].create(**kwargs)

class WebScraper(Thread):
    """
    Abstract class that defines a threaded WebScraper that may or may not run headless.
    Extend this class for each browser.
    """
    def __init__(self, headless=False,start_url=None, timeout=5, id=-1):
        """

        :param headless: True if you want your browser to run headless and false if you want to see the browser running
        :param start_url: str start url
        :param timeout: 5 if you want
        :param id: int id the web scraper should use.  If it is -1, generate a new id from MessagingQueue
        """
        # Initialize the thread
        Thread.__init__(self)
        self.create_driver()
        self.timeout = timeout
        self.driver.implicitly_wait(self.timeout)
        self.headless = headless
        if id == -1:
            self.id = MessagingQueue.unique_id()
            print('unique id is now %s ' % str(self.id))
        else:
            self.id = id
        if headless:
            self.set_headless()
        self.start_url = start_url
        self._stop_event = threading.Event()

    def get_id(self):
        """
        returns the id of the WebScraper
        :return: int id > 0
        """
        return self.id

    def stop(self):
        """
        Signals the WebScraper event to stop the WebScraper.
        :return:
        """
        self._stop_event.set()

    def stopped(self):
        """
        Returns True is the WebScraper is stopped
        Returns False if the WebScraper is not stopped.
        :return:
        """
        return self._stop_event.is_set()

    @abstractmethod
    def set_headless(self):
        """
        sets the scraper to be headless
        override for each browser
        """
        pass

    @abstractmethod
    def create_driver(self):
        """
        creates the appropriate web driver
        """
        pass


    def quit(self):
        """
        Called on a WebScraper in order to quit the WebsScraper and relinquish its resources.
        :return:
        """
        try:
            self.driver.quit()
            self.stop()
            return True
        except:
            traceback.print_exc()
            return False

    def respond(self, obj=None, command=None):
        """
        Sends a message to the main thread.
        :param obj: dict to send to the main thread
        :param command: str command being used.
        :return:
        """
        MessagingQueue.send_msg(id=self.get_id() , direction='out', cmd=command, d=obj, caller=str(self.get_id()))

    def run(self):
        """
        Run the thread
        """
        try:
            if not self.start_url is None:
                self.driver.get(self.start_url)
        except:
            traceback.print_exc()
            print('error opening start url of %s ' % self.start_url)
        ERROR_MESSAGE = False
        QUIT = False
        print('[%s] Scraper running...' % str(self.get_id()))
        while not ( ERROR_MESSAGE or QUIT ) and not self.stopped():
            print('[%s] Thread message loop ' % str(self.get_id()))
            command, obj = MessagingQueue.wait_for_msg(id=self.id, direction='in', caller=str(self.get_id()) )
            print('command in')
            print('[%s] :: command in %s with message %s '  %  (str(self.get_id()), command, str(obj)))
            if not self.stopped() or command == 'QUIT':
                print('[%s] Thread command either thread has been stopped or command is QUIT {%s} ' % ( str(self.get_id()), command ) )
                obj['driver'] = self.driver
                obj['web_driver'] = self
                response_object = ScraperComponentFactory.createScraperComponent(**obj).exec(**obj)
                if response_object is None:
                    print(command)
                    print(obj)
                    self.respond( obj=obj, command=command )
                else:
                    self.respond( obj=response_object, command=command )
            else:
                print(  'no longer active...quitting...'  )
        if not  self.stopped():
            print('This Web Scraper %s is no longer active.  Shutting down. ' % self.id )

        q = self.quit()
        print('id[%s] quitting.....[%s]' % ( str(self.id), str(q)))
        MessagingQueue.quit(id=self.get_id(), caller=str(self.get_id()))

class FirefoxWebScraper(WebScraper):
    """
    Firefox version of webscraper
    """
    def __init__(self, headless=False, start_url=None, timeout=HitParadeBot.DEFAULT_TIMEOUT, id=-1):
        """
        FirefoxWebScrpaer Constructor
        :param headless: True if you want your browser to run headless and false if you want to see the browser running
        :param start_url: str start url
        :param timeout: 5 if you want
        :param id: int id the web scraper should use.  If it is -1, generate a new id from MessagingQueue
        """
        super().__init__(headless, start_url, timeout, id)

    def create_driver(self):
        """
        Creates the browser specific driver in this instance Firefox
        :return:
        """
        self.driver = webdriver.Firefox()

    def set_headless(self):
        """
        sets the scraper to be headless
        override for each browser
        """
        os.environ['MOZ_HEADLESS'] = '1'

    class Factory:
        """
        Factory class used by the WebScraperFactory
        """
        def create(self, **kwargs): return FirefoxWebScraper(**kwargs)

class ChromeWebScraper(WebScraper):
    """
    Chrome version of webscraper
    """
    def __init__(self, headless=True, start_url=None, timeout=HitParadeBot.DEFAULT_TIMEOUT, id=-1):
        """
        Chrome Constructor
        :param headless: True if you want your browser to run headless and false if you want to see the browser running
        :param start_url: str start url
        :param timeout: 5 if you want
        :param id: int id the web scraper should use.  If it is -1, generate a new id from MessagingQueue
        """
        super().__init__(headless, start_url, timeout, id)

    def create_driver(self):
        """
        Creates the browser specific driver in this instance Chrome
        :return:
        """
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('window-size=1200x600')
        self.driver = webdriver.Chrome(chrome_options=self.options)

    def set_headless(self):
        """
        sets the scraper to be headless
        override for each browser
        """
        self.options.add_argument('headless')
        self.options.add_argument('-headless')

    class Factory:
        """
        Factory class used by the WebScraperFactory
        """
        def create(self, **kwargs): return FirefoxWebScraper(**kwargs)

#WebScraperComponentCommandFactory
class WebScraperComponentCommandFactory:
        command_mapping = {
            'QUIT': 'WebScraperComponentQuitCommand',
            'OPEN': 'WebScraperComponentOpenCommand',
            'SCRAPE': 'WebScraperComponentScrapeCommand'
        }
        factories = {}

        def addFactory(command, scraperCommandFactory):
            WebScraperComponentCommandFactory.factories.put[command] = scraperCommandFactory

        addFactory = staticmethod(addFactory)

        def createScraperCommand(**kwargs):
            hash_value = WebScraperComponentCommandFactory.command_mapping.get(kwargs.get('command', None), None)
            if hash_value is None:
                print('error hash value is None for %s.' % kwargs.get('command', None))
            elif not hash_value is None and not kwargs.get('command', None) in WebScraperComponentCommandFactory.factories:
                WebScraperComponentCommandFactory.factories[kwargs.get('command', None)] = eval( hash_value + '.Factory()')
            return WebScraperComponentCommandFactory.factories[kwargs.get('command', None)].create(**kwargs)

class WebScraperComponentCommand:
        """
        Base class of WebScraperComponentCommand
        Must impliment execute_web_method
        Used by ScraperComponent
        """
        def __init__(self, **kwargs):
            self.command = kwargs.get('command', None)
            self.action = kwargs.get('action', None)

        def get_result_message(self, success):
            if success:
                return {
                    'command': self.command,
                    'message' :'Successfully ' + self.command + '.'
                }
            else:
                return {
                    'command': self.command,
                    'message': 'Failed to ' + self.command + '.'
                }

        @abstractmethod
        def execute_web_command(self, **kwargs):
            pass

class WebScraperComponentQuitCommand(WebScraperComponentCommand):
        """
        Class to shutdown a WebScraper.  Used by ScraperComponent through a factory method.
        """
        def __init__(self, **kwargs):
            super().__init__(**kwargs)

        def execute_web_command(self, **kwargs):
            if self.command == 'QUIT':
                return self.get_result_message(self.action.quit())

        class Factory:
            def create(self, **kwargs): return WebScraperComponentQuitCommand(**kwargs)

class WebScraperComponentOpenCommand(WebScraperComponentCommand):
        """
        Class to open a url.  Used by ScraperComponent through a factory method.
        """
        def __init__(self, **kwargs):
            super().__init__(**kwargs)

        def execute_web_command(self, **kwargs):
            if self.command == 'OPEN':
                return self.get_result_message(self.action.open())

        class Factory:
            def create(self, **kwargs): return WebScraperComponentOpenCommand(**kwargs)

class WebScraperComponentScrapeCommand(WebScraperComponentCommand):
        """
        Class to create a WebScrape.  Used by ScraperComponent through a factory method.
        """
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.successful = False

        def is_successful(self):
            return self.successful

        def execute_web_command(self, **kwargs):
            el = None
            if self.command == 'SCRAPE':
                self.action.open()
                for i in range(self.action.retry_count):
                    if el is None:
                        try:
                            try:
                                if self.action.wait_for:
                                    while el is None:
                                        el = self.action.driver.find_element_by_css_selector(self.action.selector)
                                        time.sleep(self.action.time_delay)
                                else:
                                    el = self.action.driver.find_element_by_css_selector(self.action.selector)
                            except:
                                traceback.print_exc()
                                pass
                            if not el is None:
                                el.click()
                                self.successful = True
                        except:
                            traceback.print_exc()
                            pass
            success_value = not el is None and self.is_successful()
            if success_value:
                MessagingQueue.send_msg_nowait(id=self.action.web_driver.id,direction='meta',cmd='SUCCESS', d={'selector' : self.action.selector}, caller='WebScraperComponentScrapeCommand')
            return self.get_result_message(success_value)

        class Factory:
            def create(self, **kwargs): return WebScraperComponentScrapeCommand(**kwargs)

#ScraperComponents
class ScraperComponentFactory:
    """
    ScraperComponentFactory creates all ScraperComponents
    """
    factories = {}
    def addFactory(type_id, scraperFactory):
        """
        Adds a scraper factory
        :param type_id:
        :param scraperFactory:
        :return:
        """
        ScraperComponentFactory.factories.put[type_id] = scraperFactory
    addFactory = staticmethod(addFactory)

    def createScraperComponent(type_id=None,**kwargs):
        """
        creates a scraper component based upon kwargs
        :param kwargs: dict of data to create a ScraperComponent
        :return: ScraperComponent
        """
        if not type_id in ScraperComponentFactory.factories:
            ScraperComponentFactory.factories[type_id] = eval(type_id+'.Factory()')
        return ScraperComponentFactory.factories[type_id].create(**kwargs)

class ScraperComponent:
    """
    Basic Scraper Component Class.
    Basic things that inputing data and scraping data will need.
    """
    def __init__(self,**kwargs):
        """
        Constructor for all ScraperComponents
        :param **kwargs dict using the following data.
                driver:  WebScraper driver default None
                web_driver: WebScraper  default None
                scraper_url: str url to open default None
                open_url: bool True to open upon startup and False it waits.
                command:  str command this component responds to and runs.
                default_parser: HitParadeParser defaults to BeautifulSoupParser
                retry_count: int number of times to retry anything default HitParadeBot.DEFAULT_RETRY
        """
        self.driver = kwargs.get('driver', None)
        self.scraper_url = kwargs.get('scraper_url', None)
        self.retry_count = kwargs.get('retry_count', HitParadeBot.DEFAULT_RETRY)
        self.command = kwargs.get('command', None)
        self.open_url = kwargs.get('data_selectors', {}).get('open_url', True)
        parser_kwargs = {'driver' : self.driver}
        self.default_parser = HitParadeParserFactory.createParser(kwargs.get('default_parser', 'BeautifulSoupParser'), **kwargs)
        #scraper component web driver
        self.web_driver = kwargs.get('web_driver', None)
        print('ScrapeComponent init open()')
        self.force_refresh = kwargs.get('force_refresh', False)
        self.open()

    @abstractmethod
    def exec(self,**kwargs):
        """
        Method that is called by the WebScraper
        :param kwargs: dict of data
        :return:
        """
        pass

    @abstractmethod
    def perform_action(self, **kwargs):
        """
        Action for ScraperComponent
        :param kwargs: dict of data
        :return:
        """
        pass

    def respond(self, val):
        """
        respond to main thread
        :param val: dict to send bac
        :return:
        """
        if not self.web_driver is None:
            self.web_driver.respond(obj=val, command=self.command)

    def quit(self):
        """
        ScraperComponet Quit
        :return:
        """
        try:
            self.web_driver.quit()
            return True
        except:
            return False

    def get_driver(self):
        """
        returns the driver
        :return: WebScraper driver
        """
        return self.driver

    def get_scraper_url(self):
        """
        Returns the str scraper url
        :return: str scraper_url
        """
        return self.scraper_url

    def get_retry_count(self):
        """
        returns retry count
        :return: int retry_count
        """
        return self.retry_count

    def get_open_url(self):
        """
        returns the open url
        :return: bool True or False to open upon start.
        """
        return self.open_url


    def open(self):
        """
        Opens the open url if necessary.
        :return: bool True if opened and False if not opened.
        """
        try:
            if ((not self.open_url) or self.force_refresh) and not self.driver is None and not self.scraper_url is None:
                print( 'refresh %s ' % self.scraper_url )
                self.driver.get(self.scraper_url)
                self.default_parser.reload_content()
                self.open_url = True
                return True
        except:
            traceback.print_exc()
        return False

class ScraperAction(ScraperComponent):
    """
    ScraperAction is what a bot uses to click buttons, enter inputs and call other ScraperComponents.
    It can also scroll or use javascript.
    Class that will open the url if necessary.
    Find the selected selector.
    And action it in the form of a button.
    Custom Actions should extend this class.
    """
    def __init__(self,**kwargs):
        """
        Constructor for ScraperAction
        :param driver:  WebScraper driver
        :param web_driver: WebScraper
        :param scraper_url: str url
        :param selector: str selector
        :param command: str command being run.
        :param retry_count: int number of times to retry an action.
        """
        """
        Constructor for all ScraperComponents
        :param **kwargs dict using the following data.
                driver:  WebScraper driver default None
                web_driver: WebScraper  default None
                scraper_url: str url to open default None
                open_url: bool True to open upon startup and False it waits.
                command:  str command this component responds to and runs.
                default_parser: HitParadeParser defaults to BeautifulSoupParser
                retry_count: int number of times to retry anything default HitParadeBot.DEFAULT_RETRY
                **    selector: str selector to use inside this ScraperAction
        """
        super().__init__(**kwargs)
        self.selector = kwargs.get('selector', None)
        self.wait_for = kwargs.get('wait_for', False)
        self.time_delay = kwargs.get('time_delay', 3)

    def exec(self,**kwargs):
        """
        Overridden exec from ScraperComponent

        :param kwargs:
        :return:
        """
        return self.perform_action(**kwargs)


    def perform_action(self, **kwargs):
        """
        Performs action sepecified.

        :return: True if performed and False if not successfully performed
        """
        if not kwargs.get('driver', None) is None:
            self.driver =  kwargs.get('driver', None)
        if self.command is None:
            self.command = 'SCRAPE'
        command_dict = {
            'action' : self,
            'command' : self.command
        }
        return WebScraperComponentCommandFactory.createScraperCommand(**command_dict).execute_web_command(**command_dict)


    class Factory:
        def create(self, **kwargs): return ScraperAction(**kwargs)

class ScraperInputFields(ScraperComponent):
    """
    Class that inputs fields into the forms on a page.
    Override this if you need to perform custom inputs.
    """
    def __init__(self,**kwargs):
        """
        Constructor for all ScraperComponents
        :param **kwargs dict using the following data.
                driver:  WebScraper driver default None
                web_driver: WebScraper  default None
                scraper_url: str url to open default None
                open_url: bool True to open upon startup and False it waits.
                command:  str command this component responds to and runs.
                default_parser: HitParadeParser defaults to BeautifulSoupParser
                retry_count: int number of times to retry anything default HitParadeBot.DEFAULT_RETRY
        """
        super().__init__(**kwargs)
        self.input_values = kwargs.get('input_values',None)

    def input_all_values(self, driver=None):
        """
        Input the values n the forms.
        For weird fields, override this function with a custom ScraperInputField
        :return True if all the fields were input False if one was not
        """
        if not driver is None:
            self.driver = driver
        for key,value in self.input_values.items():
            try:
                self.driver.find_element_by_css_selector(key).send_keys(value)
            except:
                traceback.print_exc()
                print('exception setting %s to %s ' % (key,value))
                return False
        return True

    def exec(self,**kwargs):
        """
        Overridden exec from ScraperComponent

        :param kwargs:
        :return:
        """
        input_vals = self.input_all_values(**kwargs)
        self.respond(input_vals)
        return input_vals

    class Factory:
        def create(self, **kwargs): return ScraperInputFields(**kwargs)

class ScraperLogin(ScraperComponent):
    """
    Python class that will log into an interface
    """
    def __init__(self, **kwargs ):
        """
               Constructor for all ScraperComponents
               :param **kwargs dict using the following data.
                       driver:  WebScraper driver default None
                       web_driver: WebScraper  default None
                       scraper_url: str url to open default None
                       open_url: bool True to open upon startup and False it waits.
                       command:  str command this component responds to and runs.
                       default_parser: HitParadeParser defaults to BeautifulSoupParser
                       retry_count: int number of times to retry anything default HitParadeBot.DEFAULT_RETRY
                       ** selectors: Selectors for input fields
                       ** login_page_selectors: Selectors to deem if we are on the specific login page
                       ** retry_count: number of times to retry inputting fields or clicking a button
        """
        super().__init__(  **kwargs )
        self.login_page_selectors = kwargs.get('login_page_selectors', None)
        self.selectors = kwargs.get('selectors', None)
        self.login_button_selector = kwargs.get('login_button_selector', None)
        kwargs['selector'] = self.login_button_selector
        self.login_button_action = ScraperAction(**kwargs)


    def login_if_necessary(self, **kwargs):
        """
        Determines if you are on the login page.
        If you are - login and return True
        Otherwise do nothing and return False
        :return: True if login was performed and False if not logged in or if not on the login page.
        """
        if not kwargs.get('driver',None) is None:
            self.driver = kwargs.get('driver',None)
        try:
            scraper_input = ScraperInputFields(driver=self.driver,input_values=self.selectors,retry_count=self.retry_count, web_driver=kwargs.get('web_driver', None))
            if self.is_on_login_page(**kwargs):
                input = scraper_input.input_all_values(driver=self.driver)
                if input:
                    action_performed = False
                    for i in range(self.retry_count):
                        if not action_performed:
                            action_performed = self.login_button_action.exec(**kwargs)

                    if action_performed:
                        return {
                            'command':  'LOGIN',
                            'message': 'Login succeeded.'
                        }
                    else:
                        return {
                            'command': 'LOGIN',
                            'message': 'Login failed.'
                        }
            else:
                return {
                    'command': 'LOGIN',
                    'message': 'Login not attempted.'
                }
        except:
            print('error logging in  see trace.')
            traceback.print_exc()
            return {
                'command' : 'LOGIN',
                'message' : 'Login failed'
            }

    def exec(self,**kwargs):
        """
        Overridden exec from ScraperComponent

        :param kwargs:
        :return:
        """
        login_vals = self.login_if_necessary(**kwargs)
        self.respond(login_vals)
        return login_vals

    def is_on_login_page(self,**kwargs):
        """
        Selectors to define whether we are on  the login page
        All selectors must be found
        :return: True if the driver is on  the login page
        """
        if not kwargs.get('driver',None) is None:
            self.driver =  kwargs.get('driver',None)
        for key  in self.login_page_selectors:
            try:
               c = self.driver.find_element_by_css_selector(key)
               if c is None:
                   return False
            except:
                return False        ##just not on login page -- no big deal
            return True

    class Factory:
        def create(self, **kwargs): return ScraperLogin(**kwargs)

class Scraper(ScraperComponent):
    """
    Class that if necessary logs and then scrapes data.
    When it completes the thread will complete.
    This scraper could scrape continually or just one time.
    """

    def __init__(self, **kwargs):
        """
        Constructor for all ScraperComponents
        :param **kwargs dict using the following data.
                driver:  WebScraper driver default None
                web_driver: WebScraper  default None
                scraper_url: str url to open default None
                open_url: bool True to open upon startup and False it waits.
                command:  str command this component responds to and runs.
                default_parser: HitParadeParser defaults to BeautifulSoupParser
                retry_count: int number of times to retry anything default HitParadeBot.DEFAULT_RETRY
                ** scraper_logins: Login components that check if they are on the login page and login.
                ** selectors: Selectors to scrape
                ** sleep_time time to sleep between scrapes if and only if residual defaults to 5 seconds
        """
        super().__init__( **kwargs )
        self.scraper_logins = list( map(lambda l: ScraperComponentFactory.createScraperComponent('ScraperLogin',**l), kwargs.get('scraper_logins', [])))
        self.scraper_prerun_actions = list(map(lambda l: ScraperComponentFactory.createScraperComponent('ScraperAction',**l) , kwargs.get('scraper_prerun_actions', [])))
        self.scraper_postrun_actions = list(map(lambda l: ScraperComponentFactory.createScraperComponent('ScraperAction',**l) , kwargs.get('scraper_postrun_actions', [])))
        self.data_selectors = kwargs.get('data_selectors', {}).get('data_selectors', [])
        self.sleep_time = kwargs.get('sleep_time', HitParadeBot.DEFAULT_RETRY)
        self.force_refresh =  kwargs.get('data_selectors', {}).get('force_refresh', False)
        self.default_parser = BeautifulSoupParser(**kwargs)
        if self.scraper_url is None and not self.data_selectors.get('scraper_url', None) is None:
            self.scraper_url = self.data_selectors.get('scraper_url', None)
        self.login_if_necessary(**kwargs)
        self.start_scrape_iteration = 1
        self.end_scrape_iteration = 10
        self.current_scrape_iteration = 1
        self.run_postrun_actions_to_reset = kwargs.get('run_postrun_actions_to_reset', True)

    def login_if_necessary(self, **kwargs):
        """
        Logs the scraper in if it is necessary.
        :param kwargs: dict data if necessary
        :return:
        """
        try:
            self.open()
            if not self.scraper_logins is None:
                for login_val in self.scraper_logins:
                    if login_val.is_on_login_page():
                        login_kwargs = dict()
                        login_kwargs.update(kwargs)
                        login_kwargs['web_driver'] = self.web_driver
                        login_val.login_if_necessary(**login_kwargs)
        except:
            traceback.print_exc()
            print('error opening scrape url %s ' % self.scraper_url)

    def scrape_basic_selector(self,selector=None, sub_element=None,attributes=None,scrape_types=None,iframe_index=0 ,log_exceptions=True):
        """
        Returns an atomic element and all properties requested.
        :param selector: str selector
        :param sub_element: element sub_element to choose from. Or not if it is None
        :param attributes: list[] of attributes to choose.
        :param scrape_types: list[] types of infromation to choose.
        :param iframe_index: int index of iframe to use if necessary.
        :param log_exceptions: bool True to print exceptiosn False to not print exceptions.
        :return: dict values selected.  Empty dict if nothing is found.
        """
        selector_vals = dict()
        if len(attributes) > 0 and not 'attributes' in scrape_types:
            scrape_types.append('attributes')
        try:
            selector_vals[selector] = self.default_parser.get_atomic_element(scrape_types=scrape_types,
                                                                                          selector=selector,
                                                                                          attributes=attributes,
                                                                                          sub_element=sub_element,
                                                                                          iframe_index=iframe_index,
                                                                                          log_exceptions=log_exceptions)
        except:
            if log_exceptions:
                traceback.print_exc()
        return selector_vals

    def extract_to_attributes(self,
                                selector=None,
                                prefix=None,
                                data_selector_dict=None,
                                selector_separator='>',
                                default_selectors=[],
                                default_attributes=[],
                                default_multiple=False,
                                default_types=['text']):
        selector_key = selector# if prefix is None else prefix + selector_separator + selector
        internal_selectors = data_selector_dict.get(selector + '.selectors', default_selectors)
        iframe_properties =  data_selector_dict.get(selector + '.iframe', {})
        attributes = data_selector_dict.get(selector + '.attributes', default_attributes)
        scrape_types = data_selector_dict.get(selector + '.types', default_types)
        selector_multiple = data_selector_dict.get(selector + '.multiple', default_multiple)
        if selector_multiple:
            if not 'elements' in scrape_types:
                scrape_types.append('elements')
        return selector_key, internal_selectors, attributes, scrape_types, iframe_properties, selector_multiple


    def reformat_selector(self, card_item_listing=None, filtered_props=['message']):
        card_dict = dict()
        for card_item_props in card_item_listing:
            for k,v in card_item_props.items():
                                        if isinstance(v, dict):
                                            for k1,v1 in v.items():
                                                if isinstance(v1, dict) and len(v1.items()) > 0:
                                                    card_dict[k] = v1
                                                    card_dict[k1] = v1
                                                else:
                                                    card_dict[k] = v1
                                                    card_dict[k1] = v1
                                        elif not k in filtered_props:
                                            if not v is None and isinstance(v, dict) and len(v.items()) > 0:
                                                card_dict[k] = v
        return card_dict


    def scrape_one_element(self, selector=None, sub_element=None, attributes=None, scrape_types=None, iframe_index=0, log_exceptions=False ):
        scrape_property = dict()
        try:
            scrape_property = self.scrape_basic_selector(selector=selector, sub_element=sub_element,
                                                                    attributes=attributes, scrape_types=scrape_types,
                                                                    iframe_index=iframe_index )
        except:
            if log_exceptions:
                traceback.print_exc()
        return scrape_property

    def scrape_nested_selectors(self,prefix=None,selector=None,data_selector_dict=None, sub_element=None, scrape_types=['text'], log_exceptions=True):
        def add_new_property(aggregate_dict, new_dict, s):
            if not aggregate_dict is None:
                if not new_dict is None and len(new_dict.items()) > 0:
                    v = new_dict.get(s,None)
                    if not v is None:
                        if isinstance(v, dict):
                            if len(v.items()) > 0:
                                aggregate_dict.update(new_dict)
                        else:
                            aggregate_dict.update(new_dict)
            return aggregate_dict

        scraped_data = dict()
        selector_key, internal_selectors,   attributes, scrape_types_, iframe_properties, is_multiple = self.extract_to_attributes(selector=selector, prefix=prefix, data_selector_dict=data_selector_dict,default_types=scrape_types, default_multiple=False)
        if is_multiple:
            all_elements = self.default_parser.get_atomic_element(scrape_types=scrape_types_,
                                                   selector=selector_key,
                                                   attributes=attributes,
                                                   iframe_index=iframe_properties.get('index', 0),
                                                   sub_element=sub_element,
                                                   log_exceptions=log_exceptions)

            if not all_elements is None and len(all_elements.get('element', [])) > 0:
                element_data = []
                for idx, root_element in enumerate( all_elements.get('element', []) ):
                    node_properties = dict()
                    for i, k in enumerate(all_elements.keys()):
                        if not k == 'element':
                            node_properties[k] = all_elements[k][idx]
                    scrape_types_ = [x for x in scrape_types_ if not x == 'elements']
                    if not internal_selectors is None and len(internal_selectors) > 0:
                        for internal_selector in internal_selectors:
                            sub_element_scraped_data = self.scrape_nested_selectors(prefix=selector,
                                                                                    selector=internal_selector,
                                                                                    data_selector_dict=data_selector_dict,
                                                                                    scrape_types=scrape_types_,
                                                                                    sub_element=root_element)
                            if not sub_element_scraped_data is None and sub_element_scraped_data.get(internal_selector, None) is None:
                                new_dict = {
                                    internal_selector: sub_element_scraped_data
                                }
                                sub_element_scraped_data = new_dict

                            node_properties = add_new_property(node_properties, sub_element_scraped_data, internal_selector)


                        if not node_properties is None and len(node_properties.items()) > 0:
                            element_data.append(node_properties)
                    else:
                        select_one_element_data = self.scrape_one_element(selector=selector, sub_element=sub_element, attributes=attributes,
                                                    scrape_types=scrape_types_,
                                                    iframe_index=iframe_properties.get('index', 0),
                                                    log_exceptions=log_exceptions)
                        scraped_data = add_new_property(scraped_data, select_one_element_data, selector)
                if len(element_data) > 0:
                    scraped_data[selector] = element_data
            else:
                if log_exceptions:
                    print(' Error selector %s did not have elements on this page. ' % selector)
        else:
            if len(internal_selectors) == 0:
                internal_selector_data = self.scrape_one_element(selector=selector, sub_element=sub_element,attributes=attributes, scrape_types=scrape_types_, iframe_index=iframe_properties.get('index',0), log_exceptions=log_exceptions)
                scraped_data = add_new_property(scraped_data, internal_selector_data, selector)
            else:
                if not internal_selectors is None and len(internal_selectors) > 0:
                    for internal_selector in internal_selectors:
                        sub_element_scraped_data = self.scrape_nested_selectors(prefix=selector,
                                                                                selector=internal_selector,
                                                                                data_selector_dict=data_selector_dict,
                                                                                scrape_types=scrape_types_,
                                                                                sub_element=sub_element)
                        scraped_data = add_new_property(scraped_data, sub_element_scraped_data, internal_selector)
        return scraped_data

    def run_prerun_actions(self, **kwargs):
        for action in self.scraper_prerun_actions:
            self.run_action(action=action,**kwargs)

    def run_postrun_actions(self, **kwargs):
        for action in self.scraper_postrun_actions:
            self.run_action(action=action,**kwargs)

    def run_action(self, action=None, **kwargs):
        try:
            action.exec(**kwargs)
        except:
            traceback.print_exc()

    def should_run_reset(self):
        return self.run_postrun_actions_to_reset and self.current_scrape_iteration > self.start_scrape_iteration and len(self.scraper_postrun_actions) > 0

    def update_postrun_iteration(self, **kwargs):
        pass

    def scrape_data(self, **kwargs):
        """
        Override this if you want to scrape differently.
        This is just generic
        :return:
        """
        while self.should_run_reset():
            self.run_postrun_actions(**kwargs)
            self.update_postrun_iteration(**kwargs)

        scraped_data = dict()
        # if kwargs.get('response_queue', None) is None:
        #     scraped_data['command'] = 'SCRAPE'
        self.login_if_necessary()
        self.run_prerun_actions(**kwargs)
        if not self.data_selectors is None:
            for data_selector_dict in self.data_selectors:
                if not data_selector_dict.get('scraper_url', None) is None and not data_selector_dict.get('scraper_url', None) ==self.scraper_url:
                    self.scraper_url = data_selector_dict['scraper_url']
                    print('scrape_data --> open  ' )
                    self.open()

                for selector in data_selector_dict.get('selectors', []):
                    try:
                        scraped_data_dict = self.scrape_nested_selectors(prefix=None,
                                                                                  selector=selector,
                                                                                  data_selector_dict=data_selector_dict,
                                                                                  sub_element=None)
                        if not scraped_data_dict is None and isinstance(scraped_data_dict, dict):
                            scraped_data.update(scraped_data_dict)
                        else:
                            scraped_data[selector] = scraped_data_dict
                    except:
                        traceback.print_exc()
        for k in scraped_data:
            if not scraped_data.get(k,None) is None and isinstance(scraped_data.get(k,None) , dict):
                d = scraped_data.get(k,None)
                remove_keys = []
                for k1 in d:
                    if not isinstance(d.get(k1, None), str):
                        remove_keys.append(k1)
                for rk in remove_keys:
                    d.pop(rk,None)
        self.run_postrun_actions(**kwargs)
        return scraped_data

    def exec(self,**kwargs):
        scraper_vals = self.scrape_data(**kwargs)
        self.respond(scraper_vals)
        return scraper_vals

    class Factory:
        def create(self, **kwargs):
            scraper_kwargs = dict()
            scraper_kwargs.update(kwargs)
            scraper_kwargs['scraper_url'] =  kwargs.get('data_selectors', {}).get('scraper_url', None)
            logins = kwargs.get('scraper_logins',[])
            scraper_logins = []
            for login in logins:
                login['driver'] = kwargs.get('driver', None)
                login['web_driver'] = kwargs.get('web_driver', None)
                login['scraper_url'] = kwargs.get('data_selectors', {}).get('scraper_url', None)
            scraper_prerun_actions = kwargs.get('data_selectors', {}).get('scraper_prerun_actions', [])
            scraper_postrun_actions = kwargs.get('data_selectors', {}).get('scraper_postrun_actions', [])
            for a in scraper_prerun_actions:
                a['driver'] = kwargs.get('driver', None)
                a['web_driver'] = kwargs.get('web_driver', None)
            for a in scraper_postrun_actions:
                a['driver'] = kwargs.get('driver', None)
                a['web_driver'] = kwargs.get('web_driver', None)
                # scraper_logins.append(ScraperComponentFactory.createScraperComponent(type_id='ScraperLogin', **login))
            scraper_kwargs['scraper_logins'] = scraper_logins
            scraper_kwargs['scraper_prerun_actions'] = kwargs.get('data_selectors', {}).get('scraper_prerun_actions', [])
            scraper_kwargs['scraper_postrun_actions'] = kwargs.get('data_selectors', {}).get('scraper_postrun_actions', [])
            return Scraper(**scraper_kwargs)

#HitParadeParsers
class HitParadeParserFactory:
    """
    HitParadeOutputFactory creates all HitParadeOutput components
    """
    factories = {}
    def addFactory(type_id, parserFactory):
        """
        Adds a scraper factory
        :param type_id:
        :param outputFactory:
        :return:
        """
        HitParadeParserFactory.factories.put[type_id] = parserFactory
    addFactory = staticmethod(addFactory)

    def createParser(type_id=None,**kwargs):
        """
        creates a scraper component based upon kwargs
        :param kwargs: dict of data to create a ScraperComponent
        :return: ScraperComponent
        """
        if not type_id in HitParadeParserFactory.factories:
            HitParadeParserFactory.factories[type_id] = eval(type_id+'.Factory()')
        return HitParadeParserFactory.factories[type_id].create(**kwargs)

class HitParadeParser:
    """
    This is the base class of all ScraperComponent html parsers.
    """
    def __init__(self, **kwargs):
        """
        Constructor
        :param kwargs: dict data to use
        """
        self.driver = kwargs.get('driver', None)

    @abstractmethod
    def reload_content(self):
        """
        Reloads content for the parser if a url changes.
        :return:
        """
        pass

    @abstractmethod
    def get_text(self, css_selector=None, sub_element=None, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        pass

    @abstractmethod
    def get_elements(self, css_selector=None,sub_element=None ,is_multiple=False, log_exceptions=False):
        """
        returns elements based upon css selector.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param is_multiple: bool True if multiple elements and False if only one element.
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of element
        """
        pass

    @abstractmethod
    def get_attributes(self, css_selector=None, sub_element=None, attributes=[],is_multiple=False, log_exceptions=False):
        """
        returns elements based upon css selector.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param attributes: list [] of attributes to retrieve.
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of attributes
        """
        pass

    def get_atomic_element(self,scrape_types=['text'],selector=None, iframe_index=0, attributes=[],sub_element=None,iframe_css='iframe',log_exceptions=True):
        """
        returns a dict of all tye types of selections based upon css selector and sub element and/or iframe.
        :param scrape_types: list[] types to retrieve    text,attributes,element(s),iframe
        :param selector: str css selector
        :param iframe_index: int index of iframe if necessary
        :param attributes: list[] list of attributes to retrieve if necessary
        :param sub_element: element to select against.  do not select against it if it is None
        :param iframe_css: str css to find the iframe.
        :param log_exceptions: bool True if exceptions should be logged and false otherwise.
        :return:
        """
        vals = dict()
        ismultiple = 'elements' in scrape_types
        if 'iframe' in scrape_types:
            iframe_value = self.get_elements(css_selector=iframe_css, sub_element=sub_element, log_exceptions=log_exceptions, is_multiple=False)
            se = sub_element
            if not iframe_value is None and len(iframe_value.items()) > 0:
                se = None
                self.driver.switch_to.frame(iframe_index)
            elements_values = self.get_atomic_element(scrape_types=[x for x in scrape_types if not x == 'iframe'], selector=selector, attributes=attributes, sub_element=se, iframe_css=iframe_css, log_exceptions=log_exceptions)
            vals.update(elements_values)
            if not iframe_value is None and len(iframe_value.items()) > 0:
                self.driver.switch_to.default_content()
        if 'text' in scrape_types:
            text_values = self.get_text(css_selector=selector, is_multiple=ismultiple, sub_element=sub_element, log_exceptions=log_exceptions)
            vals.update(text_values)
        if 'attributes' in scrape_types or len(attributes) > 0:
            attribute_values = self.get_attributes(css_selector=selector, sub_element=sub_element, is_multiple=ismultiple, attributes=attributes, log_exceptions=log_exceptions)
            vals.update(attribute_values)
        if 'element' in scrape_types:
            element_value = self.get_elements(css_selector=selector, sub_element=sub_element, log_exceptions=log_exceptions, is_multiple=False)
            vals.update(element_value)
        if 'elements' in scrape_types:
            elements_values = self.get_elements(css_selector=selector, sub_element=sub_element, log_exceptions=log_exceptions, is_multiple=True)
            vals.update(elements_values)
        return vals

class BeautifulSoupParser(HitParadeParser):
    """
    HitParadeParser that combines Selenium with BeatifulSoup framework.
    BeautifulSoup parses much faster than Selenium on average.
    BeautifulSoup cannot click buttons etc.
    """
    def __init__(self,**kwargs):
        """
        Constructor
        :param kwargs: dict data to retrieve info if necessary.
        """
        super().__init__(**kwargs)
        html = self.driver.page_source
        self.soup = BeautifulSoup(html, "html.parser")

    def reload_content(self):
        """
        Reload content
        :return:
        """
        html = self.driver.page_source
        self.soup = BeautifulSoup(html)

    def get_text(self, css_selector=None, is_multiple=False, sub_element=None, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        element_dict = dict()
        element = self.get_elements(css_selector=css_selector, is_multiple=is_multiple, sub_element=sub_element,log_exceptions=log_exceptions )
        if not element is None and not element.get('element', None) is None:
            if is_multiple:
                element_dict['text'] = []
                for e in element.get('element' , []):
                    try:
                        element_dict['text'].append({'text' : str(e.text).strip()})
                    except:
                        if log_exceptions:
                            traceback.print_exc()
            else:
                try:
                    element_dict['text'] = str(element.get('element', None).text).strip()
                except:
                    if log_exceptions:
                        traceback.print_exc()
        return element_dict

    def get_elements(self, css_selector=None, sub_element=None, is_multiple=False, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        element_dict = dict()
        try:
            if is_multiple:
                if sub_element is None:
                    element_dict['element'] = self.soup.select(css_selector)
                else:
                    element_dict['element'] = sub_element.select(css_selector)
            else:
                if sub_element is None:
                    element_dict['element'] = self.soup.select_one(css_selector)
                else:
                    element_dict['element'] = sub_element.select_one(css_selector)
        except:
            if log_exceptions:
                traceback.print_exc()
        return element_dict


    def get_attributes(self, css_selector=None, sub_element=None,is_multiple=False, attributes=[], log_exceptions=False):
        """
         returns elements based upon css selector.
         :param css_selector: str css selector
         :param sub_element: element if None, do not select off sub_element
         :param attributes: list [] of attributes to retrieve.
         :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
         :return: dict with the property of attributes
         """
        element_dict = dict()
        if len(attributes) > 0:
            element = self.get_elements( css_selector=css_selector, sub_element=sub_element,
                                        log_exceptions=log_exceptions,is_multiple=is_multiple )
            if not element is None and not element.get('element', None) is None:
                if is_multiple:
                    for attribute in attributes:
                        element_dict[attribute] = []
                        for e in element.get('element', []):
                            try:
                                element_dict[attribute].append( e[attribute] )
                            except:
                                if log_exceptions:
                                    traceback.print_exc()
                else:
                    for attribute in attributes:
                        try:
                            element_dict[attribute] = str(element.get('element', None)[attribute])
                        except:
                            if log_exceptions:
                                traceback.print_exc()
        return element_dict

    class Factory:
        def create(self, **kwargs): return BeautifulSoupParser(**kwargs)

class SeleniumParser(HitParadeParser):
    """
    HitParadeParser that uses Selenium
    """
    def __init__(self,**kwargs):
        """
        Constructor
        :param kwargs: dict data to retrieve info if necessary.
        """
        super().__init__(**kwargs)

    def reload_content(self):
        """
        Reload content
        :return:
        """
        html = self.driver.page_source

    def get_text(self, css_selector=None, sub_element=None, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        element_dict = dict()
        element = self.get_elements(css_selector=css_selector, sub_element=sub_element,log_exceptions=log_exceptions, is_multiple=False)
        if not element is None and not element.get('element', None) is None:
            try:
                element_dict['text'] = element.get('element', None).text
            except:
                if log_exceptions:
                    traceback.print_exc()
        return element_dict

    def get_elements(self, css_selector=None, sub_element=None, is_multiple=False, log_exceptions=False):
        """
        Retrieve text from a css element on a page.
        :param css_selector: str css selector
        :param sub_element: element if None, do not select off sub_element
        :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
        :return: dict with the property of text
        """
        element_dict = dict()
        try:
            if is_multiple:
                if sub_element is None:
                    element_dict['element'] = self.driver.find_elements_by_css_selector(css_selector)
                else:
                    element_dict['element'] = sub_element.find_elements_by_css_selector(css_selector)
            else:
                if sub_element is None:
                    element_dict['element'] = self.driver.find_element_by_css_selector(css_selector)
                else:
                    element_dict['element'] = sub_element.find_element_by_css_selector(css_selector)
        except:
            if log_exceptions:
                traceback.print_exc()
        return element_dict

    def get_attributes(self, css_selector=None, sub_element=None, is_multiple=False, attributes=[], log_exceptions=False):
        """
         returns elements based upon css selector.
         :param css_selector: str css selector
         :param sub_element: element if None, do not select off sub_element
         :param attributes: list [] of attributes to retrieve.
         :param log_exceptions: bool True to print exceptions and False not to print Exceptions.
         :return: dict with the property of attributes
         """
        element_dict = dict()
        if len(attributes) > 0:
            element = self.get_elements(css_selector=css_selector, sub_element=sub_element, log_exceptions=log_exceptions, is_multiple=is_multiple)
            if not element is None and not element.get('element' , None) is None:
                for attribute in attributes:
                    try:
                        element_dict[attribute] = element.get('element', None).get_attribute(attribute)
                    except:
                        if log_exceptions:
                            traceback.print_exc()
        return element_dict

    class Factory:
        def create(self, **kwargs): return SeleniumParser(**kwargs)