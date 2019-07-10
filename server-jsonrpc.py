# -*- coding: utf-8 -*-
from functionsex import *

from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple

from jsonrpc import JSONRPCResponseManager, dispatcher

from VombatiDB import Workspace

import errors as storeError
import api, apiWrapper

@Request.application
def application(request):
   response=JSONRPCResponseManager.handle(request.data, dispatcher)
   return Response(response.json, mimetype='application/json')

def main():
   workspace=Workspace()
   myApi=ClassFactory(api.ApiBase, (
      api.ApiAccount,
      api.ApiLabel,
      api.ApiFilter,
   ), metaclass=apiWrapper.ApiWrapperJSONRPC)(workspace)
   #
   for k in dir(myApi):
      if k[0]=='_': continue
      v=getattr(myApi, k)
      if not callable(v): continue
      #? here we can convert names
      dispatcher[k]=v
   #
   run_simple('localhost', 4000, application)

if __name__=='__main': main()
