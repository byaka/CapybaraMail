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
   def userId(name):
      if isinstance(name, str):
         if name.startswith('user#'): return name
         else:
            return 'user#%s'%name
      raise ValueError('Incorrect type')

   @staticmethod
   def contactlistId(index):
      if isinstance(index, str) and index.startswith('contactlist#'): return index
      elif isinstance(index, int):
         return 'contactlist#%s'%index
      raise ValueError('Incorrect type')

   @staticmethod
   def contactId(index):
      if isinstance(index, str) and index.startswith('contact#'): return index
      elif isinstance(index, int):
         return 'contact#%s'%index
      raise ValueError('Incorrect type')

   @staticmethod
   def dateId(date):
      if isinstance(date, str) and date.startswith('date#'): return date
      elif isinstance(date, (datetime.date, datetime.datetime)):
         return 'date#'+date.strftime('%Y%m%d')
      raise ValueError('Incorrect type')

   @staticmethod
   def dialogId(index):
      if isinstance(index, str) and index.startswith('dialog#'): return index
      elif isinstance(index, int):
         return 'dialog#%s'%index
      raise ValueError('Incorrect type')

   def userAdd(self, user, password, descr=None, avatar=None):
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
      try:
         self.db.set(userId, data, strictMode=True, onlyIfExist=False)
         self.db.set((userId, 'node_label'), False, strictMode=True, onlyIfExist=False)
         self.db.set((userId, 'node_date'), False, strictMode=True, onlyIfExist=False)
         self.db.set((userId, 'node_contactlist'), False, strictMode=True, onlyIfExist=False)
         self.db.set((userId, 'node_shared_user'), False, strictMode=True, onlyIfExist=False)
         self.contactlistAdd(userId, 'ALL', contactlist=0)
         self.contactAdd(userId, 0, 'SELF', contact=0)
      except dbError.ExistStatusMismatchError:
         raise StoreError(-101)
      return userId

   def contactlistAdd(self, user, name, descr=None, color=None, contactlist=NULL):
      userId=self.userId(user)
      _suf='+' if contactlist is NULL else ('#%i'%contactlist)
      contactlistId=self.db.set((userId, 'node_contactlist', 'contactlist'+_suf), {
         'name':name,
         'descr':descr,
         'color':color,
      }, strictMode=True)[-1]
      return contactlistId

   def contactAdd(self, user, contactlist, nameFirst, nameSecond=None, avatar=None, note=None, contact=NULL):
      userId=self.userId(user)
      contactlistId=self.contactlistId(contactlist)
      _suf='+' if contact is NULL else ('#%i'%contact)
      contactId=self.db.set((userId, 'node_contactlist', contactlistId, 'contact'+_suf), {
         'nameFirst':'str',
         'nameSecond':nameSecond,
         'avatar':avatar,
         'note':note,
      }, strictMode=True)[-1]
      self.db.set((userId, 'node_contactlist', contactlistId, contactId, 'node_msg_in'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_contactlist', contactlistId, contactId, 'node_msg_out'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_contactlist', contactlistId, contactId, 'node_field'), False, strictMode=False, onlyIfExist=False)
      return contactId

   def contactFieldModify(self, user, contactlist, contact, fieldType, value, name=None, desc=None, **additional):
      if fieldType=='email':
         fieldId=self.fieldId_email(value)
         data={
            'value':value,
            'name':name,
            'descr':descr,
         }
      else:
         #! нужна возможность легко расширять поддержку полей через сабклассинг
         raise ValueError('Unsupported filed type: %s'%fieldType)
      userId=self.userId(user)
      contactlistId=self.contactlistId(contactlist)
      contactId=self.contactId(contact)
      self.db.set((userId, 'node_contactlist', contactlistId, contactId, 'node_field', fieldId), data, strictMode=True)
      return fieldId

   def contactFindByField(self):
      #! пока неясно как это реализовать
      pass

   def dateAdd(self, user, date):
      userId=self.userId(user)
      dateId=self.dateId(date)
      self.db.set((userId, 'node_date', dateId), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_msg_in'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_msg_out'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_dialog'), False, strictMode=False, onlyIfExist=False)
      return (userId, 'node_date', dateId)

   def dialogAdd(self, user, date):
      userId=self.userId(user)
      dateId=self.dateId(date)
      ids=self.db.set((userId, 'node_date', dateId, 'node_dialog', 'dialog+'), False, strictMode=True)
      return ids

   def msgAdd(self, user, isIncoming, msg, body, params, raw, label=None, strictMode=True):
      data={
         'subject':params['subject'],
         'timestamp':params['timestamp'],
         'isIncoming':bool(isIncoming),
         'raw':raw,
         'body':body,
      }
      addContacts={}
      #
      name, val=params['from']
      data['from']=val
      addContacts[val]=name
      #
      data['to']=[]
      for name, val in params['to']:
         addContacts[val]=name
         data['to'].append(val)
      data['to']=tuple(data['to'])
      #
      data['cc']=[]
      for name, val in params['cc']:
         addContacts[val]=name
         data['cc'].append(val)
      data['cc']=tuple(data['cc'])
      #
      data['bcc']=[]
      for name, val in params['bcc']:
         addContacts[val]=name
         data['bcc'].append(val)
      data['bcc']=tuple(data['bcc'])
      #
      data['attachments']=None  #! fixme
      #
      addLabels=[]  #! fixme
      #
      dialogIds=self.dialogAdd(userId, dateId)  #! search by `in-reply-to`
      #
      userId=self.userId(user)
      dateId=self.dateAdd(userId, params['timestamp'])
      msgId=self.msgId(msg)
      target=(userId, 'node_date', dateId, 'node_msg_'%('in' if isIncoming else 'out'), msgId)
      try:
         self.db.set(target, data, strictMode=True, onlyIfExist=False)
         self.db.link(dialogIds+(msgId,), target, strictMode=True, onlyIfExist=False)
         #
         addContacts  #! fixme
         addLabels  #! fixme
      except dbError.ExistStatusMismatchError:
         raise  #! fixme



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
         'from':email.utils.parseaddr,  #! у адресов первая часть тоже может быть закодирована
         'to':email.utils.parseaddr,
         'cc':email.utils.parseaddr,
         'bcc':email.utils.parseaddr,
         'date':email.utils.parsedate_tz,
         'subject':self._decodeHeader,
         'x-gmail-labels':self._decodeHeader,
      }

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
