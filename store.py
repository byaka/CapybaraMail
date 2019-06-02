# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import VombatiDB, showDB, showStats, Workspace
from VombatiDB import errors as dbError

from scheme import SCHEME
from errors import StoreError, AccessDeniedError

class StoreBase(object):
   def __init__(self, workspace=None, **kwargs):
      self._main_app=sys.modules['__main__']
      self.inited=False
      self.started=False
      self.settings_frozen=False
      self.workspace=workspace or Workspace()
      self.settings=self._settings=MagicDictCold({})
      self.supports=self._supports=MagicDictCold({})
      self._init(**kwargs)
      self._inited(**kwargs)

   def _init(self, **kwargs):
      if not hasattr(self.workspace, 'dbPath'):
         self.workspace.dbPath=getScriptPath(real=True, f=__file__)+'/db'
      self.settings.reinitNamespacesOnStart=True

   def _preinited(self, **kwargs):
      self.inited=True

   def start(self, **kwargs):
      self._settings=dict(self.settings)
      self.settings._MagicDictCold__freeze()
      self._supports=dict(self.supports)
      self.supports._MagicDictCold__freeze()
      self.settings_frozen=True
      self._start(**kwargs)
      self.started=True

   def _start(self, **kwargs):
      self.db=VombatiDB(('NS', 'Columns', 'MatchableLinks', 'StorePersistentWithCache', 'Search'))(self.workspace, self.workspace.dbPath)
      self._configureDB(reinitNamespaces=self._settings['reinitNamespacesOnStart'])

   def _configureDB(self, reinitNamespaces):
      self.db.settings.store_flushOnChange=False
      self.db.settings.ns_checkIndexOnConnect=True
      self.db.settings.dataMerge_ex=True
      self.db.settings.dataMerge_deep=False
      self.db.settings.linkedChilds_default_do=False
      self.db.settings.linkedChilds_inheritNSFlags=True
      self.db.settings.ns_default_allowLocalAutoIncrement=False
      self.db.settings.columns_default_allowUnknown=False
      self.db.settings.columns_default_allowMissed=False
      self.db.connect()
      if reinitNamespaces:
         self.db.configureNS(SCHEME, andClear=True)

   @staticmethod
   def userId(user):
      if isinstance(user, str):
         if user.startswith('user#'): return user
         else:
            return 'user#%s'%user
      raise ValueError('Incorrect type')

   @staticmethod
   def dateId(date):
      if isinstance(date, str) and date.startswith('date#'): return date
      elif isinstance(date, (datetime.date, datetime.datetime)):
         return 'date#'+date.strftime('%Y%m%d')
      raise ValueError('Incorrect type')

   @staticmethod
   def emailId(email):
      if isinstance(email, str):
         if email.startswith('email#'): return email
         else:
            return 'email#%s'%email
      raise ValueError('Incorrect type')

   @staticmethod
   def dialogId(index):
      if isinstance(index, str) and index.startswith('dialog#'): return index
      elif isinstance(index, int):
         return 'dialog#%s'%index
      raise ValueError('Incorrect type')

   def userAdd(self, user, password, descr=None, avatar=None, strictMode=True):
      assert isStr(password) and password
      userId=self.userId(user)
      if self.db.isExist(userId):
         raise LogicError(-106)
      passwordHash=password  #! здесь добавить хеширование
      data={
         '_passwordHash':passwordHash,
         '_connector':None,
         'isActive':True,
         'name':user,
         'descr':descr,
         'avatar':avatar,
      }
      self.db.set(userId, data, strictMode=strictMode, onlyIfExist=False)
      self.db.set((userId, 'node_date'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_email'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_label'), False, strictMode=False, onlyIfExist=False)
      return (userId,)

   def dateAdd(self, user, date, strictMode=False):
      userId=self.userId(user)
      dateId=self.dateId(date)
      self.db.set((userId, 'node_date', dateId), False, strictMode=strictMode, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_email'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_dialog'), False, strictMode=False, onlyIfExist=False)
      return (userId, 'node_date', dateId)

   def emailAdd(self, user, email, name=None, strictMode=False):
      userId=self.userId(user)
      emailId=self.emailId(email)
      self.db.set((userId, 'node_email', emailId), False, strictMode=strictMode, onlyIfExist=False)
      return (userId, 'node_email', emailId)

   def dialogAdd(self, user, date, strictMode=False):
      userId=self.userId(user)
      dateId=self.dateId(date)
      ids=self.db.set((userId, 'node_date', dateId, 'node_dialog', 'dialog+'), False, strictMode=strictMode)
      return ids

   def msgAdd(self, user, isIncoming, msg, body, params, raw, label=None, strictMode=True):
      userId=self.userId(user)
      _linkTo=[]
      data={
         'subject':params['subject'],
         'timestamp':params['timestamp'],
         'isIncoming':bool(isIncoming),
         'raw':raw,
         'body':body,
      }
      #
      dateIds=self.dateAdd(userId, params['timestamp'], strictMode=strictMode)
      #
      isFrom=True
      for k in ('from', 'to', 'cc', 'bcc'):
         v=params.get(k) or None
         if isFrom:
            data[k], v=v, (v,)
         else:
            data[k]=tuple(_v[1] for _v in v) if v else None
         for name, email in v:
            emailIds=self.emailAdd(user, email, name, strictMode=strictMode)
            emailIds=self.db.link(
               dateIds+('node_email', emailIds[-1]),
               emailIds, strictMode=strictMode, onlyIfExist=False)
            if isFrom:
               msgIds=emailIds+(self.msgId(msg),)
            else:
               _linkTo.append(emailIds)
         isFrom=False
      #
      #! search by `in-reply-to`
      dialogIds=self.dialogAdd(userId, dateIds[-1])
      _linkTo.append(dialogIds)
      #
      data['attachments']=None  #! fixme
      #
      self.db.set(msgIds, data, strictMode=strictMode, onlyIfExist=False)
      for ids in _linkTo:
         self.db.link(
            ids+(msgIds[-1]),
            msgIds, strictMode=strictMode, onlyIfExist=False)
      #
      #? большой вопрос - как хранить вложенные лэйблы. подход gmail кажется весьма неплохим
      # if label:
      #    toNow=
      #    for l in label:
      return msgIds

   # def userEdit(self, user, descr=NULL, avatar=NULL):
   #    userId=self.userId(user)
   #    if descr is NULL and avatar is NULL: return
   #    data={}
   #    if descr is not NULL: data['descr']=descr
   #    if avatar is not NULL: data['avatar']=avatar
   #    try:
   #       self.db.set(userId, data, allowMerge=True, strictMode=True, onlyIfExist=True)
   #    except dbError.ExistStatusMismatchError:
   #       raise LogicError(-104)

import mailbox, email

class ImportMBox(object):

   _headers=('date', 'from', 'to', 'cc', 'bcc', 'message-id', 'in-reply-to', 'references', 'reply-to', 'archived-at', 'sender', 'x-gmail-labels', 'subject')

   def __init__(self, path):
      self.path=path
      self._headers_preprocess={
         'from':self._parseAddress,
         'to':self._parseAddress,
         'cc':self._parseAddress,
         'bcc':self._parseAddress,
         'date':email.utils.parsedate_tz,
         'subject':self._decodeHeader,
         'x-gmail-labels':self._parseLabels,
      }

   @classmethod
   def _parseAddress(cls, data):
      print '   >', data, email.utils.parseaddr(data)
      return tuple(
         (self._decodeHeader(n), v)
         for n, v in email.utils.parseaddr(data)
      )

   @classmethod
   def _parseLabels(cls, data):
      return tuple(
         (tuple(
            ss.strip() for ss in s.split('/')
         ) if '/' in s else s.strip())
         for s in self._decodeHeader(v).split(',')
      )

   @classmethod
   def _decodeText(cls, obj):
      t=obj.get_content_type()
      if t=='text/plain' or t=='text/html':
         return obj.get_payload(decode=True)
      else:
         print '! unknown type', t
         return False

   @classmethod
   def getBody(cls, message):
      if message.is_multipart():
         for part in message.walk():
            if part.is_multipart():
               for subpart in part.walk():
                  data=cls._decodeText(subpart)
                  if data is not False:
                     return data
            else:
               data=cls._decodeText(part)
               if data is not False:
                  return data
      else:
         data=cls._decodeText(message)
         if data is not False:
            return data
      return None

   @classmethod
   def _decodeHeader(cls, data):
      # data=re.sub(r"(=\?.*\?=)(?!$)", r"\1 ", data)  # fix broken headers, https://stackoverflow.com/a/7331577
      res=''.join(unicode(s, e or 'ASCII') for s,e in email.header.decode_header(data))
      return res

   def __iter__(self):
      for message in mailbox.mbox(self.path):
         headers={
            'charset':message.get_charset(),
         }
         # https://en.wikipedia.org/wiki/Email#Header_fields
         for k in self._headers:
            headers[k]=message.get_all(k)
            if headers[k] and k in self._headers_preprocess:
               m=self._headers_preprocess[k]
               headers[k]=tuple(m(v) for v in headers[k])
         body=self.getBody(message)
         #
         yield message, headers, body


if __name__ == '__main__':
   importer=ImportMBox('/home/byaka/Загрузки/gmail_exported/inbox.mbox')
   for _, headers, body in importer:
      for k in importer._headers:
         print k.upper()+':', decode_utf8('%s'%headers[k])
      print
      print body
      print '='*40
      raw_input()
