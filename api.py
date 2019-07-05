# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import errors as dbError

from errors import *
from store import StoreBase, StoreDB, StoreFilesLocal, StoreDB_dialogFinderEx, StoreHashing_dummy

class ApiBase(object):
   def __init__(self, workspace, **kwargs):
      self._main_app=sys.modules['__main__']
      self.inited=False
      self.started=False
      self.settings_frozen=False
      self.workspace=workspace
      self.settings=self._settings=MagicDictCold({})
      self.supports=self._supports=MagicDictCold({})
      self._init(**kwargs)
      self._inited(**kwargs)

   def _init(self, **kwargs):
      pass

   def _inited(self, store=None, **kwargs):
      self.inited=True
      self.store=store or ClassFactory(StoreBase, (
         StoreFilesLocal,
         StoreHashing_dummy,
         StoreDB,
         StoreDB_dialogFinderEx,
      ))(self.workspace)

   def start(self, **kwargs):
      self._settings=dict(self.settings)
      self.settings._MagicDictCold__freeze()
      self._supports=dict(self.supports)
      self.supports._MagicDictCold__freeze()
      self.settings_frozen=True
      self._start(**kwargs)
      self.started=True

   def _start(self, **kwargs):
      self.store.start()

class ApiAccaunt(ApiBase):

   def accauntAdd(self, login, password, descr=None, avatar=None, connector=None):
      """
      Add new accaunt.

      :param str login: Login (name) of accaunt. This also will ID of accaunt. Can contain any letters - it will be normalized automatically.
      :param str password: Password for accaunt.
      :param str|none descr: Description of accaunt (defaults to None).
      :param str|none avatar: Encoded to base64 image (defaults to None).
      :param tuple|none connector: Config for connectors (defaults to None).
      """
      self.store.userAdd(login, password, descr=descr, avatar=avatar, strictMode=True)

   def connectorAdd(self, login, name, type, config, descr=None):
      """
      Add connector to accaunt (usually connector needed for receive and send messages).

      :param str login: Login of accaunt.
      :param str name: Connector's name, will be ID of connector.
      :param str type: Type of connector, this will be used for find correct connector.
      :param dict config: Parameters for connector.
      :param str|none descr: Description of new connector (defaults to None).
      """
      pass

   def connectorList(self):
      """
      List all supported connectors with format of configs (it contain fields with `name`, `type` and `required` mark).

      :return dict:
      """
      pass

   def connectorSwitch(self, login, name, to=None):
      """
      Switch active-status of specific connector.

      :param str login: Login of accaunt.
      :param str name: Name of connector.
      :param bool|none to: New active-status. If `None`, it will switched to opposite status (defaults to None).
      """
      pass

class ApiLabel(ApiBase):

   def labelList(self, login, count=True, unreadOnly=False, byDialog=True):
      """
      List all labels, also count messages or dialogs in each (all and unread).

      :param str login: Login of accaunt.
      :param bool count: Enable counting of messages or dialogs (defaults to True).
      :param bool unreadOnly: Count only unreaded (defaults to False).
      :param bool byDialog: Count dialogs or messages (defaults to True).
      :return tuple:

      :note:
         As labels may be nested, in this case it will be tuple (or join it with `/` and pass like string). Also parents not counts items in children.

      :example:
         >>> api.labelList('user1', count=True, unreadOnly=False)
         ... (
            {'name':'Label 1', 'descr':'Just non-nested label', 'color':'red', 'countAll':0, 'countUnread':0},
            {'name':('Label 1', 'Label 2'), 'descr':'Just nested label', 'color':'#fff', 'countAll':10, 'countUnread':1},
            {'name':('Label 1', 'Label 2', 'Label 3'), 'descr':'We need to go deeper', 'color':'green', 'countAll':100, 'countUnread':3},

         )
      """
      pass

   def labelAdd(self, login, label, descr=None, color=None):
      """
      Add new label to accaunt.

      :param str login: Login of accaunt.
      :param str|tuple label: Label name or full ierarchy of names (for nested labels).
      :param str|none descr: Description for label (defaults to None).
      :param str|none color: Any representation of color (defaults to None).

      :note:
         If you want to create nested label - pass tuple of ierarchy or join it with `/` and pass like string.
      """
      pass

   def labelEdit(self, login, label, descr=None, color=None):
      """
      Edit existed label in accaunt.

      :param str login: Login of accaunt.
      :param str|tuple label: Label name or full ierarchy of names (for nested labels).
      :param str|none descr: Description for label (defaults to None).
      :param str|none color: Any representation of color (defaults to None).

      :note:
         If you want to edit nested label - pass tuple of ierarchy.
      """
      pass

class ApiFilter(ApiBase):

   def filterMessages(self, login, dates=None, query=None, limitDates=10, limitResults=10, asDialogs=True, returnFull=False):
      """
      Фильтрует сообщения по заданным критериям. Результаты группируются по датам.

      :param str login: Login of accaunt.
      :param tuple|int|date|none dates: Дата или даты, за которые ведется поиск. Для передачи промежутков дат используйте синтаксис `(date1, '+1', date2)`, а для обратного порядка `(date1, '-1', date2)`. Также возможно использовать формат `(date1, '-1', True)` - это эквиваленто перебору дат начиная с указанной и вплоть до последней в базе. Второй аргумент в промежутках задает направление перебора и шаг. Допускается использовать одновременно и промежутки дат и обычное перечисление. Дата задается либо типом `date`, либо строкой в формате `yyyymmdd`, либо строкой-константой `today`, `yesterday`, либо через unixtimestamp (в этом случае информация о времени будет отброшена). Значение `None` эквивалетно `('today', '+1', True)` (defaults to None).
      :param dict query:

      :param int limitDates: Ограничение на количество не пустых дней в результатах.
      :param int limitResults: Ограничение на количество сообщений (или диалогов при `asDialogs==True`) в результатах.
      :param bool asDialogs: Позволяет получать полностью диалоги вместо отдельных сообщений. При этом в результатах появится дополнительный массив с идентификаторами сообщений, непосредственно попавших под условия фильтрации.
      :param bool returnFull: Позволяет получить сообщения целиком, а не только их идентификаторы.
      :return list:

      :note:
         Параметр limitResults не может разбить одну дату. Это значит, что если передать в него `10`, а в первой обработанной дате будет 100 писем - то все 100 вернутся в результат и поиск завершится.
      """
      """
      {
         'or':[
            {'key':'label', 'value':'label1', 'match':'=='},
            {'and':[
               {'key':'label', 'value':'label2', 'match':'!='},
               {'or':[
                  {'key':'from', 'value':'from1', 'match':'=='},
                  {'key':'from', 'value':'from2', 'match':'=='},
                  {'and':[
                     {'key':'label', 'value':'label3', 'match':'!='},
                     {'key':'from', 'value':'from3', 'match':'=='},
                  ]},
                  {'key':'label', 'value':'label4', 'match':'=='},
               ]},
            ]},
            {'key':'from', 'value':'from4', 'match':'=='},
            {'key':'from', 'value':'from5', 'match':'=='},
         ]
      }
      """
      if dates is None:
         dates=('today', '-1', True)
      userId=self.store.userId(login)
      cD=cR=0
      res=[]
      dialog_map=set() if asDialogs else None
      for date, data in self.store.dialogFindEx(userId, query, dates):
         if asDialogs:
            dateId=self.store.dateId(date)
            cM=0 if returnFull else len(data)
            targets, data=data, []
            targets=tuple(s[-1] for s in targets)
            for msg in targets:
               dialogIds=self.store.dialogFind_byMsg(userId, msg, date=dateId, asThread=False)
               dialog=dialogIds[-1]
               #! првоерить в диалогах локальный и глобальный AI
               if dialog in dialog_map: continue  #! нет использования dialog_map
               if returnFull:
                  dialog=tuple(
                     self.store.msgGet_byIds(ids, props, strictMode=False, onlyPublic=True, resolveAttachments=True, andLabels=True)
                     for ids, props
                     in self.store.dialogGet_byIds(dialogIds, returnProps=True)
                  )
                  cM+=len(dialog)
               data.append(dialog)
            res.append((date, data, targets))
         else:
            cM=len(data)
            if returnFull:
               data=self.store.msgGet(userId, data, date=date)
         #
         cD+=1
         cR+=cM
         if cD>limitDates or cR>limitResults: break
      return res
