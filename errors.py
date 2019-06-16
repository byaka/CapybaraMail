# -*- coding: utf-8 -*-
__all__=['ERROR_MSG', 'CapybaraMailError', 'AccessDeniedError', 'StoreError']

ERROR_MSG={
   # USER
   -101:'User already exist',
   -104:'Unknown user',
   -105:'Inactive user',
   -106:'Id for user overlaps',
   # OTHER
    0:'Other',
   -1:'This not implemented yet',
   -3:'Access denied',
   -4:'Not implemented',
   -6:'Incorrect input data',
   -10:'No message-Id',
}


class CapybaraMailError(Exception):
   def __init__(self, code, msg=None):
      s=ERROR_MSG.get(code, 'Unknown error')
      if msg is not None:
         s='%s: %s'%(s, msg)
      super(CapybaraMailError, self).__init__(s)
      self.code=code
      self.msg=s

   def __str__(self):
      return '<Screendesk.%s(%s)> %s'%(self.__class__.__name__, self.code, self.msg)

class AccessDeniedError(CapybaraMailError):
   def __init__(self, msg=None):
      super(AccessDeniedError, self).__init__(code=-3, msg=msg)

class NoMessageIdError(CapybaraMailError):
   def __init__(self, msg=None):
      super(NoMessageIdError, self).__init__(code=-10, msg=msg)

class IncorrectInputError(CapybaraMailError):
   def __init__(self, msg=None):
      super(IncorrectInputError, self).__init__(code=-6, msg=msg)

class NotSupportedError(CapybaraMailError):
   def __init__(self, msg=None):
      super(NotSupportedError, self).__init__(code=-4, msg=msg)

class StoreError(CapybaraMailError):
   pass
