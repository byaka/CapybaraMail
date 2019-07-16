# -*- coding: utf-8 -*-
from functionsex import *

from errors import *

class ApiWrapperBase(type):
   """
   This Meta-Class wraps each public method of Api and do some protocol-specific magic or add authentication support.

   :note:
      Auth can be disabled for whole class by attr `_noAuth`.

   :note:
      Call-wrapping can be disabled for whole class by attr `_noCall`.
   """

   def __new__(meta, className, bases, classDict):
      _noAuth=classDict.get('_noAuth', False)
      _noCall=classDict.get('_noCall', False)
      for k, v in classDict.items():
         if not k.startswith('_') and callable(v):
            classDict[k]=ApiWrapperBase.wrap(
               classDict, v,
               disableAuth=getattr(v, '_noAuth', _noAuth),
               disableCall=getattr(v, '_noCall', _noCall),
            )
      return type.__new__(meta, className, bases, classDict)

   @classmethod
   def wrap(cls, classDict, f, disableAuth=False, disableCall=False):
      disableAuth, disableCall=cls._wrapping_before(classDict, f, disableAuth=False, disableCall=False)
      @functools.wraps(f)
      def fNew(self, *args, **kwargs):
         scope=cls._wrapped_pre({}, f, self, args, kwargs)
         if not disableAuth:
            cls._wrapped_auth(scope, f, self, args, kwargs)
         if not disableCall:
            return cls._wrapped_call(scope, f, self, args, kwargs)
         else:
            return f(self, *args, **kwargs)
      fNew._original=f
      fNew=cls._wrapping_after(classDict, fNew, f)
      return fNew

   @classmethod
   def _wrapping_before(cls, classDict, fOld, disableAuth, disableCall):
      return disableAuth, disableCall

   @classmethod
   def _wrapping_after(cls, classDict, fNew, fOld):
      return fNew

   @classmethod
   def _wrapped_pre(cls, scope, f, self, args, kwargs):
      return scope

   @classmethod
   def _wrapped_auth(cls, scope, f, self, args, kwargs):
      pass

   @classmethod
   def _wrapped_call(cls, scope, f, self, args, kwargs):
      return f(self, *args, **kwargs)


#! поскольку библиотека оказалось негибкой и топорной, пока будем использовать flaskJSONRPCServer. Тогда на клиенте пока можно воспользоваться библиотекой `https://jsonrpcclient.readthedocs.io/en/latest/api.html` или `jsonrpc-requests`

from jsonrpc import dispatcher

class ApiWrapperJSONRPC(ApiWrapperBase):
   #~ это имплементация под https://pypi.org/project/json-rpc/

   @classmethod
   def _wrapping_before(cls, classDict, fOld, disableAuth, disableCall):
      if 'login' not in inspect.getargs(fOld.__code__)[0]:
         disableAuth=False
      return disableAuth, disableCall

   @classmethod
   def _wrapping_after(cls, classDict, fNew, fOld):
      if not hasattr(classDict, '_JSONRPC_dispatcherMap'):
         setattr(classDict, '_JSONRPC_dispatcherMap', dispatcher)
      classDict.dispatcher[fOld.__name__]=fNew
      return fNew

   @classmethod
   def _prepBadResp(cls, e):
      if isinstance(e, AssertionError):
         return {'code':-6, 'data':ERROR_MSG[-6]}
      elif isinstance(e, NotImplementedError):
         return {'code':-1, 'data':ERROR_MSG[-1]}
      elif isinstance(e, ScreendeskError):
         return {'code':e.code, 'data':e.msg}
      else:
         return {'code':False, 'data':getErrorInfo(fallback=False)}

   @classmethod
   def _prepResp(cls, data):
      if isinstance(data, Exception):
         return cls._prepBadResp(data)
      else:
         return {'code':True, 'data':data}

   @classmethod
   def _wrapped_pre(cls, scope, f, self, args, kwargs):
      scope['conn']=kwargs.get('_conn', {})  #! это завязано на старом FJSONRPC протоколе
      return scope

   @classmethod
   def _wrapped_auth(cls, scope, f, self, args, kwargs):
      pass

   @classmethod
   def _wrapped_call(cls, scope, f, self, args, kwargs):
      if scope['conn'].get('__getRawResponse', False):
         r=f(self, *args, **kwargs)
      else:
         try:
            r=cls._prepResp(f(self, *args, **kwargs))
         except Exception, e:
            r=cls._prepBadResp(e)
      return r
