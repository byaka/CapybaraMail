# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import errors as dbError

from errors import *
from store import StoreBase, StoreFilesLocal, StoreDB, StoreDB_dialogFinderEx, StoreHashing_dummy, StoreUtils

def makeStoreClass():
   return ClassFactory(StoreBase, (
      StoreUtils,
      StoreFilesLocal,
      StoreHashing_dummy,
      StoreDB,
      StoreDB_dialogFinderEx,
   ))

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
      self.store=store or makeStoreClass()(self.workspace)

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

class ApiAccount(ApiBase):

   def accountAdd(self, login, password, descr=None, avatar=None, connector=None):
      """
      Add new account.

      :param str login: Login (name) of account. This also will ID of account. Can contain any letters - it will be normalized automatically.
      :param str password: Password for account.
      :param str|none descr: Description of account (defaults to None).
      :param str|none avatar: Encoded to base64 image (defaults to None).
      :param tuple|none connector: Config for connectors (defaults to None).
      """
      return self.store.userAdd(login, password, descr=descr, avatar=avatar, strictMode=True)
   accountAdd._noAuth=True

   def connectorAdd(self, login, name, type, config, descr=None):
      """
      Add connector to account (usually connector needed for receive and send messages).

      :param str login: Login of account.
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

      :param str login: Login of account.
      :param str name: Name of connector.
      :param bool|none to: New active-status. If `None`, it will switched to opposite status (defaults to None).
      """
      pass

class ApiLabel(ApiBase):

   def labelList(self, login, countAll=True, countWithLabel=False, byDialog=True):
      """
      List all labels, also count messages or dialogs in each.

      :param str login: Login of account.
      :param bool countAll: Enable counting of messages or dialogs (defaults to True).
      :param list countWithLabel: Count messages that also have this label (defaults to False).
      :param bool byDialog: Count dialogs or messages (defaults to True).
      :return tuple:

      :note:
         As labels may be nested, in this case there name will be joined with `/` and pass like this.

      :note:
         Parents not counts items in children.

      :note:
         This method don't shows labels with `special==True`.

      :example python:
         >>> api.labelList('user1', countAll=True, countWithLabel=['unread'])
         ... (
            {'name':'Label 1', 'descr':'Just non-nested label', 'color':'red', 'countAll':0, 'countWithLabel':{'unread':0}},
            {'name':'Label 1/Label 2', 'descr':'Just nested label', 'color':'#fff', 'countAll':10, 'countWithLabel':{'unread':10}},
            {'name':'Label 1/Label 2/Label 3', 'descr':'We need to go deeper', 'color':'green', 'countAll':100, 'countWithLabel':{'unread':3}},

         )
      """
      return tuple(self.store.labelList(login, countAll=countAll, countWithLabel=countWithLabel, byDialog=byDialog))

   def labelAdd(self, login, label, descr=None, color=None):
      """
      Add new label to account.

      :param str login: Login of account.
      :param str|tuple label: Label name or full ierarchy of names (for nested labels).
      :param str|none descr: Description for label (defaults to None).
      :param str|none color: Any representation of color (defaults to None).

      :note:
         If you want to work with nested label - pass tuple of ierarchy or join it with `/` and pass like string.
      """
      return self.store.labelAdd(login, label, descr=descr, color=color, strictMode=False)

   def labelEdit(self, login, label, descr=None, color=None):
      """
      Edit existed label in account.

      :param str login: Login of account.
      :param str|tuple label: Label name or full ierarchy of names (for nested labels).
      :param str|none descr: Description for label (defaults to None).
      :param str|none color: Any representation of color (defaults to None).

      :note:
         If you want to work with nested label - pass tuple of ierarchy or join it with `/` and pass like string.
      """
      pass

   def labelMark(self, login, msg, label, andClear=False):
      """
      Mark message with specific label(s) and optionally clear other labels from it.

      :param str login: Login of account.
      :param str msg: Message's id.
      :param str|list|tuple|none label: Label's ids or `None`.
      :param bool andClear: Also clear all other labels (defaults to False).

      :note:
         If you want to work with nested label - pass tuple of ierarchy or join it with `/` and pass like string.
      """
      pass

   def labelUnmark(self, login, msg, label):
      """
      Remove specific label(s) from message.

      :param str login: Login of account.
      :param str msg: Message's id.
      :param str|list|tuple|none label: Label's ids or `None`.

      :note:
         If you want to work with nested label - pass tuple of ierarchy or join it with `/` and pass like string.
      """
      pass

class ApiFilter(ApiBase):

   def filterMessages(self, login, dateIterator=None, query=None, limitDates=10, limitResults=10, asDialogs=True, returnFull=False, onlyCount=False, checkDialogDate=True, blacklist=None, returnDateIterator=True, returnProcessed=False):
      """
      Фильтрует сообщения по заданным критериям. Результаты группируются по датам.

      :param str login: Login of account.
      :param tuple|none dateIterator:
         Промежутки дат, за который ведется поиск. Каждый промежуток состоит из трех элементов - дата начала, шаг и дата конца.
         Также возможно использовать формат `(date1, -1, True)` - это эквиваленто перебору дат начиная с указанной и вплоть до самой старой в базе (при положительном шаге в качестве даты-конца будет использована сама новая дата в базе).
         Дата задается либо типом `date`, либо строкой в формате `yyyymmdd`, либо строкой-константой `today`, `yesterday`, `today+1`, `today-4` (и так далее).
         Значение `None` эквивалетно `('today', -1, True)` (defaults to None).
      :param dict query:
         Запрос, состоящий из вложенных словарей и списков. Элементы запроса имееют следующий формат:
            * `{'or':[..]}` являющийся логическим оператором **или** (где в список вложены иные операторы)
            * `{'and':[..]}` являющийся логическим оператором **и**
            * `{'key':'key_name', 'match':'==', 'value':'value'}` задающий условие фильтрации.

         Поддерживается фильтрация по следующим ключам: **from**, **to**, **label**. Для атрибута `match` допускается также значение `!=`, ознаающиее **не равно** (defaults to None).
      :param int limitDates: Ограничение на количество не пустых дней в результатах (defaults to 10).
      :param int limitResults: Ограничение на количество сообщений (или диалогов при `asDialogs==True`) в результатах (defaults to 10).
      :param bool asDialogs: Позволяет получать полностью диалоги вместо отдельных сообщений. При этом в результатах появится дополнительный массив с идентификаторами сообщений, непосредственно попавших под условия фильтрации (defaults to True).
      :param bool returnFull: Позволяет получить сообщения целиком, а не только их идентификаторы (defaults to False).
      :param bool onlyCount: Вместо самих диалогов (сообщений) возвращает количество (defaults to False).
      :param bool checkDialogDate: Для режима обработки диалогов (`asDialogs==True`) включает проверку, попадает ли диалог в граници дат. Датой диалога всегда считается дата последнего сообщения в нем. Использование данного флага позволяет избежать дублирования диалогов в выдаче между запросами, в ситуации когда сообщения за разные даты находятся в одном диалоге (defaults to True).
      :param tuple|list|none blacklist: Сюда можно передать идентификаторы сообщений или диалогов (в зависимости от флага `asDialogs`), которые будут пропущены при обработке.
      :param bool returnDateIterator: Добавляет в результаты модифицированный `dateIterator`, в котором остались только необработанные даты из запрошенных. Работает с любыми комбинациями дат (defaults to True).
      :param bool returnProcessed: Добавляет в результат список идентификаторов обработанных обьектов плюс значения из `blacklist` если были переданы. Это позволяет избавляться от дубликатов обьектов между разными запросами (defaults to False).
      :return list:

      :note:
         Параметр `limitResults` не может разбить одну дату. Это значит, что если передать `limitResults=10`, а за какуюто дату найдется 100 писем - то все 100 вернутся в результат и поиск завершится.

      :example python:
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

         # query matchs for this

         msg.label == 'label1' or (
            msg.label == 'label2' or(
               msg.from == 'from1' or
               msg.from == 'from2' or(
                  msg.label == 'label3' and msg.from == 'from3'
               )
            )
         ) or
         msg.from == 'from4' or
         msg.from == 'from5'
      """
      #? удаление дубликатов диалогов через checkDialogDate отлично подойдет для всех случаев кроме режима поиска по конкретной дате. однако поскольку dateIterator больше не принимает отдельные даты - эта проблема актуальна только для режимов поиска когда явно переданы граници дат для поиска
      if dateIterator is None:
         dateIterator=('today', -1, True)
      _needTargets=asDialogs and not onlyCount
      userId=self.store.userId(login)
      cD=cR=0
      resData=[]
      resTargets=[] if _needTargets else None
      blacklistMap=set(blacklist) if blacklist else set()
      dialogInBounds={}
      g=None
      dialogFindEx=self.store.dialogFindEx
      for date, data, g in dialogFindEx(userId, query, dateIterator):
         dateId=self.store.dateId(date)
         if asDialogs:
            cM=0 if returnFull else len(data)
            msgs, targets, data=data, [], []
            for msgIds in msgs:
               dialogIds=self.store.dialogFind_byMsgIds(msgIds, asThread=False)
               dialogId=self.store.dialogIds2human(dialogIds)
               if dialogId in blacklistMap: continue
               blacklistMap.add(dialogId)
               if returnFull and not onlyCount:
                  dialog=[]
                  gDialog=self.store.dialogGet_byIds(dialogIds, returnProps=True, needSortByDate=True)
                  if checkDialogDate:
                     if dialogId not in dialogInBounds:
                        gDialog=gDialog if isinstance(gDialog, (list, tuple)) else tuple(gDialog)
                        o=self.store.msgGet_byIds(*gDialog[-1], wrapMagic=False)
                        dialogInBounds[dialogId]=g.send((dialogFindEx.CMD_CHECK_DATE, o['timestamp']))
                     if not dialogInBounds[dialogId]: continue
                  for ids, props in gDialog:
                     o=self.store.msgGet_byIds(ids, props, strictMode=True, onlyPublic=True, resolveAttachments=True, andLabels=True, andDialogId=False, wrapMagic=False)
                     o['dialogId']=dialogId
                     dialog.append(o)
               else:
                  #! добавить поддержку `blacklistMap`
                  dialog=tuple(self.store.dialogGet_byIds(dialogIds, returnProps=False))
               if not dialog: continue
               cM+=len(dialog)
               data.append(dialog)
               targets.append(self.store.ids2human(msgIds))
            if _needTargets:
               resTargets.extend(targets)
         else:
            cM=len(data)
            if returnFull and not onlyCount:
               #! добавить поддержку `blacklistMap`
               data=tuple(self.store.msgGet(userId, msg, date=dateId) for msg in data)
            else:
               #! добавить поддержку `blacklistMap`
               data=tuple(data)
         resData.append((date, len(data) if onlyCount else data))
         #
         cD+=1
         cR+=cM
         if cD>limitDates or cR>limitResults: break
      r=(resData, resTargets) if _needTargets else (resData,)
      if returnDateIterator:
         # extracting date-generator for next search
         r+=(g.send(dialogFindEx.CMD_PACK_DATES),) if g else (False,)
      if returnProcessed:
         r+=(blacklistMap,)
      return r if len(r)>1 else r[0]
