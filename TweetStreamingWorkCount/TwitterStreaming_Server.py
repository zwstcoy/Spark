import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json


consumer_key = '' # Use your credential information
consumer_secret = '' # Use your credential information
access_token = '' # Use your credential information
access_secret = '' # Use your credential information


class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    # Create streaming
    twitter_stream = Stream(auth, TweetsListener(c_socket))

    # Track keyword "Friday" in twitter post
    twitter_stream.filter(track=['Friday'])


if __name__ == "__main__":
    # Create a socket object
    s = socket.socket()

    # Get local machine name
    host = "localhost"

    # Reserve a port for your service.
    port = 45670

    # Bind to the port
    s.bind((host, port))

    port = s.getsockname()
    print("Listening on port: %s" % str(port))

    # Now wait for client connection.
    s.listen(1)
    print("Waiting for a connection")

    # Establish connection with client.
    c, addr = s.accept()

    print("Received request from: " + str(addr))

    sendData(c)
