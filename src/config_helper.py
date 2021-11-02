"""Reader helper."""
import configparser

class Config_reader_helper():
    """Create helper to read configuration in an easy way."""

    config = configparser.ConfigParser()

    def __init__(self, file_config='resources/config_files/configuration.ini'):
        self.config.read(file_config)

    def get_twitter_preperties(self):
        """Return properties to connect to twitter API

        Params:

        Return:
            @twitter_properties (consumer_key, consumer_secret, access_token_key, access_token_secret)
            @type Tuple(4)
        """

        twitter_properties_dict = (
            self.config.get('twitter','consumer_key'),
            self.config.get('twitter','consumer_secret'),
            self.config.get('twitter','access_token_key'),
            self.config.get('twitter','access_token_secret'))

        return twitter_properties_dict

    def get_property_kafka(self, property_name, section_name="kafka"):
        """Return kafka property.
        
        Params:
            @param property_name -> name of the property that we want to get
            @type -> str
            @param section_name -> Section in properties file default kafka
            @type -> str

        Return:
            @property_value
            @type -> Any 
        """

        return self.config.get(section_name, property_name)

    def get_property_elasticsearch(self, property_name, section_name="elasticsearch"):
        """Return elasticsearch property.
        
        Params:
            @param property_name -> name of the property that we want to get
            @type -> str
            @param section_name -> Section in properties file default elasticsearch
            @type -> str

        Return:
            @property_value
            @type -> Any 
        """

        return self.config.get(section_name, property_name)

