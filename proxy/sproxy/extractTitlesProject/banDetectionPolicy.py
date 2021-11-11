from scrapy.exceptions import IgnoreRequest
import sys

class banDetectionPolicy(object):
    """ ban detection rules. """
    NOT_BAN_STATUSES = {200, 301, 302, 404}
    NOT_BAN_EXCEPTIONS = (IgnoreRequest,)

    def response_is_ban(self, request, response):
        if response.status not in self.NOT_BAN_STATUSES:
            print( 'response_is_ban', response, file=sys.stderr )
            return True
        if response.status == 200 and not len(response.body):
            return True
        return False

    def exception_is_ban(self, request, exception):
        #if exception:
        #    print( 'exception_is_ban?', type(exception), exception, file=sys.stderr )
        return not isinstance(exception, self.NOT_BAN_EXCEPTIONS)

