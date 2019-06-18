# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import errors as dbError

from errors import *
from store import StoreBase, StoreDB, StoreFilesLocal

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

   def _inited(self, **kwargs):
      self.inited=True
      self.store=ClassFactory(StoreBase, (StoreFilesLocal, StoreDB))(self.workspace)

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

   def messages(self, login, date=None, stopDates=10, stopResults=10, query=None, reverseSortDate=False, returnDialogs=True, returnTree=False, returnFull=False):
      #? как сделать пагинацию? планировалась пагинация по датам и временным промежуткам - такая пагинация отлично разруливается на стороне филтрации и отлично оптимизирует поиск
      #? можно ли задавать смещение типа *дата* + N первых непустых дней. тогда для подгрузки следующих резудьатов нужно передать тотже запрос, но изменить дату на последнюю в запросе + 1
      #? этот вариант кажется приемлимым, остается понять как это реализовать оптимально на этапе филтрации.
      #? например такё
      #? сперва мы генерируем запрос на основе переданного конфига, в процессе ища начальную дату если по ней идет фильтрация. а затем идем по датам начиная он начальной и выполняем совокупность операций над сетами. после чего для найденных результатов если нужно происходит ресолвинг на уровень диалогов. полученный результат мерджим с предыдущими и считаем колво непустых дат и колва общих результатов - не пора ли остановиться.
      """
      Filter messages by conditions. Results grouped by date, so you receive only results for passed dates.

      :param str login: Login of accaunt.
      :param tuple|int|datetime date: You will got results only for this dates.
      :param bool returnDialogs: If `True`, results will be grouped by dialogs.
      :param bool returnTree: Not implemented for now.
      :param bool returnFull: Switch results from dialog-ids and msg-ids only to full msgs.
      :param tuple|none byLabel: Combination of labels (defaults to None).
      :param tuple|none byDate: Combination of dates (defaults to None).
      :param bool|none byUnread: Filter by unread-status or ignore if `none` (defaults to None).
      :param tuple|none byFrom: Combination of sender's emails (defaults to None).
      :param tuple|none byTo: Combination of recipient's emails (matched in `To`, `cc`, `bcc` fields) (defaults to None).
      :return tuple:

      :note:
         This method allows to filter with `AND` and `OR` conditions. Last level is interpreted as `OR`, and previous level is `AND`.

      :note:
         If you want to use nested label - pass ierarchy, joined with `/` like string.

      :example:
         >>> api.dialogs('user1',
            byLabel=(('Label 1', 'Label 2'), ('Label 3/Label4',)),  # same as ('Label 1' or 'Label 2') and 'Label 3',
            byUnread=True,  # only unread
            byFrom=('myEmail1@test.com', 'myEmail2@test.com')  # 'myEmail1@test.com' or 'myEmail2@test.com'
         )

      :example:
         >>> api.messagesFilter('user1', byFrom=('myEmail1@test.com',), returnDialogs=True, returnTree=False, returnFull=False)
         ... (
            ('dialog1', ('msg1', 'msg2', 'msg3')),
            ('dialog2', ('msg1', 'msg2', 'msg3')),
         )

      :example:
         >>> api.messagesFilter('user1', byFrom=('myEmail1@test.com',), returnDialogs=True, returnTree=False, returnFull=True)
         ... (
            ('dialog1', (
               {'id':'msg1', 'from':'blahblah', 'subject':'blahblah'},
               {'id':'msg2', 'from':'blahblah', 'subject':'blahblah'},
               {'id':'msg3', 'from':'blahblah', 'subject':'blahblah'},
            )),
            ('dialog2', (
               {'id':'msg1', 'from':'blahblah', 'subject':'blahblah'},
               {'id':'msg2', 'from':'blahblah', 'subject':'blahblah'},
               {'id':'msg3', 'from':'blahblah', 'subject':'blahblah'},
            )),
         )

      """
      #! как реализовать `NOT` паттерны? они необходимы, но как их красиво задавать неясно
      #! в случае группировки по диалогам нужно отдельно передавать идентификаторы сообщений, которые попали под условия поиска - это позволит например автоматически развернуть эти сообщения при раскрытии диалога.
      pass

      # примеры
      OBJ=object()

      (OBJ.KEY1 == 'test2') or (OBJ.KEY1 != 'test' and OBJ.KEY2==10) or (OBJ.KEY3<=100)

      {
         'or':[
            {'key':'KEY1', 'value':'test2', 'match':'=='},
            {'and':[
               {'key':'KEY1', 'value':'test', 'match':'!='},
               {'key':'KEY2', 'value':10, 'match':'=='},
            ]},
            {'key':'KEY3', 'value':100, 'match':'<='},
         ]
      }

