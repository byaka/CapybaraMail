# -*- coding: utf-8 -*-
from functionsex import *

from VombatiDB import Workspace

import errors as storeError
import api
from apiWrapper import ApiWrapperJSONRPC

from flaskJSONRPCServer import flaskJSONRPCServer


if __name__=='__main__':
   workspace=Workspace()
   myApi=ClassFactory(api.ApiBase, (
      api.ApiAccount,
      api.ApiLabel,
      api.ApiFilter,
   ), metaclass=ApiWrapperJSONRPC)(workspace)
   myApi.start()

   server=flaskJSONRPCServer(("0.0.0.0", 7001), cors=True, gevent=False, log=3, allowCompress=False, compressMinSize=100*1024, jsonBackend='simplejson', servBackend='auto', experimental=False)
   server.registerInstance(myApi, path='/api')
   server.serveForever()
