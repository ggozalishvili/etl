import sqlalchemy


class PostgresConnection():
    def __init__(self, url, user, password):
        self._url = url
        self._user = user
        self._password = password


    def _create_engine(self):

        engine = sqlalchemy.create_engine(
             f'postgresql://{self._user}:{self._password}@{self._url}')


        return engine

    def create_connection(self):
        engine = self._create_engine()
        connection = engine.connect()
        return connection, engine


# url = "ec2-176-34-211-0.eu-west-1.compute.amazonaws.com:5432/d6j6etm7h53s01"
# user = "oswrsssbmcdtsa"
# password = 'ad6cca6bc8a6d58a80313746f2f7ad22b12ae65d7f75b447f7b785b27845d9e8'
#
#
# posgtres = PostgresConnection(url,user, password)
#
# connection = posgtres.create_connection()
# print (connection)