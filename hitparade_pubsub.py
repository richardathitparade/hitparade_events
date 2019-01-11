import redis
import time
import traceback
from abc import abstractmethod
import datetime
from threading import Thread
from registration import RegisterLeafClasses

class SegmentProcessor:

    __metaclass__ = RegisterLeafClasses
    PROCESSOR_COMMANDS = ['TODAY']

    @staticmethod
    def processor_commands():
        return SegmentProcessor.PROCCESSOR_COMMANDS

    @staticmethod
    def is_processor_command(command):
        return not command is None\
               and command in SegmentProcessor.PROCESSOR_COMMANDS \
               or command.strip() in SegmentProcessor.PROCESSOR_COMMANDS \
               or command.lower().strip() in SegmentProcessor.PROCESSOR_COMMANDS \
               or command.upper().strip() in SegmentProcessor.PROCESSOR_COMMANDS

    def __init__(self, **kwargs):
        self.segment = kwargs.get('segment', None)

    @abstractmethod
    def process_segment(self):
        pass


class TodaySegmentProcessor(SegmentProcessor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def process_segment(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    class Factory:
        def create(self,**kwargs):
            return TodaySegmentProcessor(**kwargs)

class SegmentProcessorFactory:
    factory_command_mapping = {'TODAY', 'TodaySegmentProcessor'}
    factories = {}

    @staticmethod
    def create(type_id, **kwargs):
        type_id_formatted = type_id.upper().strip()
        if not type_id_formatted in SegmentProcessorFactory.factories:
            factory_type = SegmentProcessorFactory.factory_command_mapping.get(type_id_formatted, None)
            if not factory_type is None:
                SegmentProcessorFactory.factories[type_id_formatted] = eval(factory_type + '.Factory()')
            else:
                return None
        return SegmentProcessorFactory.factories[type_id_formatted].create(**kwargs)



class RedisSubscriber(Thread):

    __metaclass__ = RegisterLeafClasses
    def __init__(self, **kwargs):
        Thread.__init__(self)
        self.redis_instance = kwargs.get('redis', None)
        self.events = kwargs.get('events', [])
        self.subscribe_all = kwargs.get('subscribe_all', False)
        self.publisher = self.redis_instance.pubsub()  # See https://github.com/andymccurdy/redis-py/#publish--subscribe
        self.preprocessed_events = []
        self.runsubscriber = True
        for event in self.events:
            pp_event, pp_events = self.__preprocess_event(event)
            if self.subscribe_all:
                self.preprocessed_events += pp_events
            else:
                self.preprocessed_events.append(pp_event)
        for ppevent in self.preprocessed_events:
            print('subscribing to event %s ' % ppevent)
            self.publisher.subscribe(ppevent)  # Subscribe to startScripts channel

    def __preprocess_event(self, event):
        return_value = []
        if not event is None:
            event_str = ''
            event_segments = event.split(RedisCache.event_separator())
            for event_segment in event_segments:
                processed_segment = str(event_segment)
                if SegmentProcessor.is_processor_command(event_segment):
                    processed_segment = SegmentProcessorFactory.create(event_segment, {'segment' : event_segment} ).process_segment()

                if event_str == '':
                    event_str = processed_segment
                else:
                    event_str += RedisCache.EVENT_SEPARATOR + processed_segment
                if self.subscribe_all:
                    return_value.append(event_str)
            return event_str, return_value
        return None

    def subscribe_to_events(self):
        for event in self.events:
            processed_event, processed_events = self.__preprocess_event(event)
            if self.subscribe_all:
                for subscribe_to_event in processed_events:
                    self.publisher.subscribe(subscribe_to_event)
            else:
                self.publisher.subscribe(processed_event)

    def run_subscriber(self):
        return self.runsubscriber

    def run(self):
        try:
            while self.run_subscriber():
                print("Waiting For redisStarter...")
                message = self.publisher.get_message()
                if message:
                    command = message['data']
                    print(command)
                    print(message)
                    self.run_subscribe(message=message)
                time.sleep(1)
            print("Permission to start...")

        except Exception as e:
            print("!!!!!!!!!! EXCEPTION !!!!!!!!!")
            print(str(e))
            print(traceback.format_exc())

    @abstractmethod
    def run_subscribe(self, message=None):
        pass

class RedisEventSubscriber(RedisSubscriber):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run_subscribe(self, message=None):
        print('message received from subsriber')
        print(message['data'])


class Team:
    def __init__(self, **kwargs):
        self.team_name_unformatted = kwargs.get('team_name_unformatted', None)
        self.team_record_unformatted = kwargs.get('team_record_unformatted', None)
        self.team_icon_unformatted = kwargs.get('team_icon_unformatted', None)
        self.__team_key = self.get_team_key()

    @abstractmethod
    def get_team_key(self):
        pass

    @abstractmethod
    def get_wins(self):
        pass

    @abstractmethod
    def get_losses(self):
        pass

    @staticmethod
    def get_abbreviations(sport):
        if not sport is None:
            sport_text = sport.upper().strip()
            if sport == 'NBA':
                return  [ 'GS', 'LAL', 'TOR', 'HOU', 'BOS', 'SA', 'OKC', 'MIL', 'PHI', 'DAL', 'ORL', 'MEM',  'ATL',  'WSH', 'NOP', 'CHA', 'IND', 'DET', 'BKN', 'LAC', 'PHX', 'POR', 'UTAH',  'SAC',  'MIN', 'CHI',  'NYK', 'CLE']
            else:
                return None
        else:
            return None

    @staticmethod
    def get_nba_team_key(team_text):
        team_name_formatted = team_text.lower().strip()
        if 'golden' in team_name_formatted or 'warrior' in team_name_formatted:
            return 'GS'
        elif 'laker' in team_name_formatted:
            return 'LAL'
        elif 'toronto' in team_name_formatted or 'raptor' in team_name_formatted:
            return 'TOR'
        elif 'houston' in team_name_formatted or 'rocket' in team_name_formatted:
            return 'HOU'
        elif 'boston' in team_name_formatted or 'celtic' in team_name_formatted:
            return 'BOS'
        elif 'spur' in team_name_formatted or 'san' in team_name_formatted and 'antonio' in team_name_formatted:
            return 'SA'
        elif 'oklahoma' in team_name_formatted or 'thunder' in team_name_formatted:
            return 'OKC'
        elif 'milwaukee' in team_name_formatted or 'bucks' in team_name_formatted:
            return 'MIL'
        elif 'philadelphia' in team_name_formatted or ('76' in team_name_formatted or 'sixer' in team_name_formatted):
            return 'PHI'
        elif 'dallas' in team_name_formatted or 'maverick' in team_name_formatted:
            return 'DAL'
        elif 'cleveland' in team_name_formatted or 'cavalier' in team_name_formatted:
            return 'CLE'
        elif ('new' in team_name_formatted and 'york') in team_name_formatted or 'knick' in team_name_formatted:
            return 'NYK'
        elif 'chicago' in team_name_formatted and 'bull' in team_name_formatted:
            return 'CHI'
        elif 'miami' in team_name_formatted or 'heat' in team_name_formatted:
            return 'MIA'
        elif 'minnesota' in team_name_formatted or (
                'timberwolves' in team_name_formatted or 'timberwolf' in team_name_formatted):
            return 'MIN'
        elif 'sacramento' in team_name_formatted or 'king' in team_name_formatted:
            return 'SAC'
        elif 'utah' in team_name_formatted or 'jazz' in team_name_formatted:
            return 'UTAH'
        elif 'portland' in team_name_formatted or 'trail' in team_name_formatted or 'blazer' in team_name_formatted:
            return 'POR'
        elif 'phoenix' in team_name_formatted and 'sun' in team_name_formatted:
            return 'PHX'
        elif 'clipper' in team_name_formatted:
            return 'LAC'
        elif 'net' in team_name_formatted or 'brooklyn' in team_name_formatted:
            return 'BKN'
        elif 'piston' in team_name_formatted or 'detroit' in team_name_formatted:
            return 'DET'
        elif 'indiana' in team_name_formatted or 'pacer' in team_name_formatted:
            return 'IND'
        elif 'charlotte' in team_name_formatted or 'hornet' in team_name_formatted:
            return 'CHA'
        elif ('new' in team_name_formatted and 'orleans' in team_name_formatted) or 'pelican' in team_name_formatted:
            return 'NOP'
        elif 'washington' in team_name_formatted or 'wizard' in team_name_formatted:
            return 'WSH'
        elif 'atlanta' in team_name_formatted or 'hawk' in team_name_formatted:
            return 'ATL'
        elif 'memphis' in team_name_formatted or 'grizzlie' in team_name_formatted:
            return 'MEM'
        elif 'orlando' in team_name_formatted or 'magic' in team_name_formatted:
            return 'ORL'
        else:
            print('error matching %s to a key original text %s ' % (team_name_formatted, team_text))
            return None

    @staticmethod
    def get_team_key_any(team_text, sport):
        if not team_text is None:
            team_key = Team.get_nba_team_key(team_text)
            if team_key is None:
                team_name_formatted = team_text.upper().strip()
                if team_name_formatted in Team.get_abbreviations(sport):
                    return team_name_formatted
                else:
                    print('no match for %s ' % team_text)
                    return None
            else:
                return team_key
        else:
            print('unable to convert none to team key')
            return None


class NbaTeam(Team):
    def __init__(self, **kwargs):
        self.team_name_unformatted = kwargs.get('team_name_unformatted', None)
        self.team_record_unformatted = kwargs.get('team_record_unformatted', None)
        self.team_icon_unformatted = kwargs.get('team_icon_unformatted', None)

    @staticmethod
    def get_team_key(team_name_unformatted):
        team_name_formatted = team_name_unformatted.lower().strip()
        if 'golden' in team_name_formatted or 'warrior' in team_name_formatted:
            return 'GS'
        elif 'laker' in team_name_formatted:
            return 'LAL'
        elif 'toronto' in team_name_formatted or 'raptor' in team_name_formatted:
            return 'TOR'
        elif 'houston' in team_name_formatted or 'rocket' in team_name_formatted:
            return 'HOU'
        elif 'boston' in team_name_formatted or 'celtic' in team_name_formatted:
            return 'BOS'
        elif 'spur' in team_name_formatted or 'san' in team_name_formatted and 'antonio' in team_name_formatted:
            return 'SA'
        elif 'oklahoma' in team_name_formatted or 'thunder' in team_name_formatted:
            return 'OKC'
        elif 'milwaukee' in team_name_formatted or 'bucks' in team_name_formatted:
            return 'MIL'
        elif 'philadelphia' in team_name_formatted or ('76' in team_name_formatted or 'sixer' in team_name_formatted):
            return 'PHI'
        elif 'dallas' in team_name_formatted or 'maverick' in team_name_formatted:
            return 'DAL'
        elif 'cleveland' in team_name_formatted or 'cavalier' in team_name_formatted:
            return 'CLE'
        elif ('new' in team_name_formatted and 'york') in team_name_formatted or 'knick' in team_name_formatted:
            return 'NYK'
        elif 'chicago' in team_name_formatted and 'bull' in team_name_formatted:
            return 'CHI'
        elif 'miami' in team_name_formatted or 'heat' in team_name_formatted:
            return 'MIA'
        elif 'minnesota' in team_name_formatted or ( 'timberwolves' in team_name_formatted or 'timberwolf' in team_name_formatted):
            return 'MIN'
        elif 'sacramento' in team_name_formatted or 'king' in team_name_formatted:
            return 'SAC'
        elif 'utah' in team_name_formatted or 'jazz' in team_name_formatted:
            return 'UTAH'
        elif 'portland' in team_name_formatted or 'trail' in team_name_formatted or 'blazer' in team_name_formatted:
            return 'POR'
        elif 'phoenix' in team_name_formatted and 'sun' in team_name_formatted:
            return 'PHX'
        elif 'clipper' in team_name_formatted:
            return 'LAC'
        elif 'net' in team_name_formatted or 'brooklyn' in team_name_formatted:
            return 'BKN'
        elif 'piston' in team_name_formatted or 'detroit' in team_name_formatted:
            return 'DET'
        elif 'indiana' in team_name_formatted or 'pacer' in team_name_formatted:
            return 'IND'
        elif 'charlotte' in team_name_formatted or 'hornet' in team_name_formatted:
            return 'CHA'
        elif ('new' in team_name_formatted and 'orleans' in team_name_formatted) or 'pelican' in team_name_formatted:
            return 'NOP'
        elif 'washington' in team_name_formatted or 'wizard' in team_name_formatted:
            return 'WSH'
        elif 'atlanta' in team_name_formatted or 'hawk' in team_name_formatted:
            return 'ATL'
        elif 'memphis' in team_name_formatted or 'grizzlie' in team_name_formatted:
            return 'MEM'
        elif 'orlando' in team_name_formatted or 'magic' in team_name_formatted:
            return 'ORL'
        else:
            print('error matching %s to a key original text %s ' % ( team_name_formatted, team_name_unformatted))
            return None



class RedisCache:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host', 'hitparade001.et5b4b.0001.use1.cache.amazonaws.com' )
        self.port = kwargs.get('port', 6379)
        self.publishers = dict()
        self.subscribers = dict()
        self.redis_instance  = redis.StrictRedis(host=self.host, port=self.port)  # Connect to local Redis instance
        self.publisher = self.redis_instance.pubsub()  # See https://github.com/andymccurdy/redis-py/#publish--subscribe
        self.pub_sub_mechanism = self.redis_instance.pubsub()

    EVENT_SEPARATOR = '.'

    @staticmethod
    def event_separator():
        return RedisCache.EVENT_SEPARATOR


    def pubsub(self):
        return self.pub_sub_mechanism

    def publish_data(self, event_string, payload):
        if not event_string is None and not payload is None:
            new_event_string = ''
            event_segments = event_string.split(RedisCache.event_separator())
            for segment in event_segments:
                if new_event_string == '':
                    new_event_string = segment
                else:
                    new_event_string += RedisCache.event_separator() + segment
                # print("Starting main scripts...")
                #cmd = self.command + ' ' + str(i)
                self.redis_instance.publish(new_event_string, payload)                  # PUBLISH START message on startScripts channel


if __name__ == '__main__':
    try:
        to = dict()
        to['host'] = 'hitparade001.et5b4b.0001.use1.cache.amazonaws.com'
        to['port'] = 6379
        to['subscribe'] = 'startScripts'
        to['publish'] = 'startScripts'
        to['command'] = u'START'
        cache = RedisCache()
        subscriber_kwargs = {
            'redis' : cache,
            'events' : ['event.sports.nba.injuries.20190109','event.sports.nba.games.20190109','event.sports.GS.injuries.20190109'  ],
            'subscribe_all': False
        }
        subscriber = RedisEventSubscriber(**subscriber_kwargs)

        subscriber.start()
        cache.publish_data('event.sports.nba.injuries.20190109', 'Butler is out today!!!')
        cache.publish_data('event.sports.nba.games.20190109', 'Score of the Hawks game is 81-71')
        cache.publish_data('event.sports.GS.injuries.20190109', 'Warriors win 3rd straight game')

    except:
        traceback.print_exc()