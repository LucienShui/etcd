import requests


class HttpResponseCodeNot200(Exception):
    pass


class Etcd:
    def __init__(self, url):
        self.__url = url + ('/' if url[-1] is not '/' else '')

    @staticmethod
    def __json_or_exception(response):
        if response.status_code != 200:
            print(response.text)
            raise HttpResponseCodeNot200
        return response.json()

    def __http_get(self, tail, **kwargs):
        return self.__json_or_exception(requests.api.get(self.__url + tail, **kwargs))

    def __http_put(self, tail, **kwargs):
        return self.__json_or_exception(requests.api.put(self.__url + tail, **kwargs))

    def __http_delete(self, tail, **kwargs):
        return self.__json_or_exception(requests.api.delete(self.__url + tail, **kwargs))

    def members(self):
        return self.__http_get('v2/members')

    def get(self, key, detail=False):
        json_response = self.__http_get('v2/keys/%s' % key)
        return json_response if detail else json_response['node']['value']

    def set(self, key, value, detail=False):
        json_response = self.__http_put('v2/keys/%s' % key, data={'value': value})
        return json_response if detail else True

    def update(self, key, value, detail=False):
        json_response = self.__http_put('v2/keys/%s' % key, data={'value': value, 'exist': 'True'})
        return json_response if detail else True

    def delete(self, key, detail=False):
        json_response = self.__http_delete('v2/keys/%s' % key)
        return json_response if detail else True

    def version(self, detail=False):
        json_response = self.__http_get('version')
        return json_response if detail else "etcdserver: %s\netcdcluster: %s" \
                                            % (json_response['etcdserver'], json_response['etcdcluster'])

    def leader(self, detail=False):
        json_response = self.__http_get('v2/stats/leader')
        return json_response if detail else json_response['leader']

    def self(self, detail=False):
        json_response = self.__http_get('v2/stats/self')
        return json_response

    def cluster_runtime(self, detail=False):
        json_response = self.__http_get('v2/stats/store')
        return json_response
    
