# -*- coding: utf-8 -*-

from functionsex import *

__all__=['DB_SCHEME', 'DB_SETTINGS']

DB_SETTINGS={
   'store_flushOnChange':False,
   'ns_checkIndexOnConnect':False,
   'dataMerge_ex':True,
   'dataMerge_deep':False,
   'linkedChilds_default_do':False,
   'linkedChilds_inheritNSFlags':True,
   'ns_default_allowLocalAutoIncrement':False,
   'columns_default_allowUnknown':False,
   'columns_default_allowMissed':False,
}

DB_SCHEME=[
#  (Name, (Parents, Childs, Columns, AllowOnlyIndexed[True], AllowOnlyNumerable[False], localAutoIncrement[fromSetts], linkChilds[fromSetts])) # noqa

   ('user', (None, ['node_date', 'node_email', 'node_label', 'node_self', 'node_dialog', 'node_problem'], {
      '_passwordHash':'str',
      'isActive':'bool',
      'name':'str',
      'descr':('str', 'none'),
      'avatar':('str', 'none'),
   }, True, False, False, None)),

   ('node_problem', ('user', 'problem', False, False, False, False, True)),

   ('problem', (['node_problem', 'msg'], None, {
      'name':'str',
      'descr':('str', 'none'),
   }, True, False, False, True)),

   ('node_self', ('user', 'email', False, False, False, False, True)),

   ('node_email', ('user', 'email', False, False, False, False, None)),

   ('node_from', ('date', 'email', False, False, False, False, True)),

   ('node_to', ('date', 'email', False, False, False, False, True)),

   ('email', (('node_email', 'node_self', 'node_from', 'node_to'), 'msg', {
      'name':('str', 'none'),
   }, True, False, False, True)),

   ('node_date', ('user', 'date', False, False, False, False, True)),

   ('date', ('node_date', ['node_from', 'node_to', 'node_dialog', 'node_msg'], False, True, True, False, None)),

   ('node_dialog', (['user', 'date'], 'dialog', False, False, False, True, None)),

   ('dialog', (['node_dialog'], ['msg'], False, True, True, True, None)),

   ('node_msg', ('date', 'msg', False, False, False, False, True)),

   #! ожидает #83 для ограничения вложенности только внутри диалогов
   ('msg', (['node_msg', 'email', 'dialog', 'msg'], ['msg', 'label', 'problem'], {
      'id':'str',
      'subject':'str',
      'timestamp':'datetime',
      'isIncoming':'bool',
      'from':'str',
      'to':('tuple', 'none'),
      'cc':('tuple', 'none'),
      'bcc':('tuple', 'none'),
      'replyTo':('str', 'none'),
      'returnPath':('str', 'none'),
      '_raw':'str',
      'bodyPlain':('str', 'none'),
      'bodyHtml':('str', 'none'),
      'attachments':('tuple', 'none'),
   }, True, False, False, None)),

   ('node_label', ('user', 'label', False, False, False, False, None)),

   ('label', (['node_label', 'label', 'msg'], ['label'], {
      '_special':'bool',
      'id':'str',
      'name':'str',
      'nameChain':'str',
      'descr':('str', 'none'),
      'color':('str', 'none'),
   }, True, False, False, None)),

   #? поидеи это легко реализуется поверх основного класса добавлением данных строк в схему и оборачиванием всех методов класс где идет итерация\получение данных. в обертах идет вызов основного метоад, а затем вычисляется к каким иным юзерам есть доступ и вызывается оригинальный метод для них -а результаты всех вызовов склеиваеются одним из способов
   # ('node_shared_user', ('user', 'shared_user', False, False, False, False, True)),
   # ('shared_user', ('node_shared_user', False, {
   #    'contactlistMap':'dict',
   # }, True, False, False, False)),
]
