# -*- coding: utf-8 -*-
import sys

from importMail import ImportMail_MBox

IS_TTY=sys.stdout.isatty()
consoleColor={
   # predefined colors
   'fail':'\x1b[91m',
   'ok':'\x1b[92m',
   'warning':'\x1b[93m',
   'okblue':'\x1b[94m',
   'header':'\x1b[95m',
   # colors
   'black':'\x1b[30m',
   'red':'\x1b[31m',
   'green':'\x1b[32m',
   'yellow':'\x1b[33m',
   'blue':'\x1b[34m',
   'magenta':'\x1b[35m',
   'cyan':'\x1b[36m',
   'white':'\x1b[37m',
   # background colors
   'bgblack':'\x1b[40m',
   'bgred':'\x1b[41m',
   'bggreen':'\x1b[42m',
   'bgyellow':'\x1b[43m',
   'bgblue':'\x1b[44m',
   'bgmagenta':'\x1b[45m',
   'bgcyan':'\x1b[46m',
   'bgwhite':'\x1b[47m',
   # specials
   'light':'\x1b[2m',
   'bold':'\x1b[1m',
   'inverse':'\x1b[7m',
   'underline':'\x1b[4m',
   'clearLast':'\x1b[F\x1b[K',
   'end':'\x1b[0m'
}
if not IS_TTY:
   consoleColor={k:'' for k in consoleColor}

#? возможно эти данные есть в `email.MIMEImage`
ATTACHMENT_TYPES={
   'audio': ['aiff', 'aac', 'mid', 'midi', 'mp3', 'mp2', '3gp', 'wav'],
   'code': ['c', 'cpp', 'c++', 'css', 'cxx', 'h', 'hpp', 'h++', 'html', 'hxx', 'py', 'php', 'pl', 'rb', 'java', 'js', 'xml'],
   'crypto': ['asc', 'pgp', 'key'],
   'data': ['cfg', 'csv', 'gz', 'json', 'log', 'sql', 'rss', 'tar', 'tgz', 'vcf', 'xls', 'xlsx'],
   'document': ['csv', 'doc', 'docx', 'htm', 'html', 'md', 'odt', 'ods', 'odp', 'ps', 'pdf', 'ppt', 'pptx', 'psd', 'txt', 'xls', 'xlsx', 'xml'],
   'font': ['eot', 'otf', 'pfa', 'pfb', 'gsf', 'pcf', 'ttf', 'woff'],
   'image': ['bmp', 'eps', 'gif', 'ico', 'jpeg', 'jpg', 'png', 'ps', 'psd', 'svg', 'svgz', 'tiff', 'xpm'],
   'video': ['avi', 'divx'],
}

ATTACHMENT_TYPES['media']=ATTACHMENT_TYPES['audio']+ATTACHMENT_TYPES['font']+ATTACHMENT_TYPES['image']+ATTACHMENT_TYPES['video']

URI_SCHEMES_PERMANENT=set((
  "data", "file", "ftp", "gopher", "http", "https", "imap",
  "jabber", "mailto", "news", "telnet", "tftp", "ws", "wss"
))

URI_SCHEMES_PROVISIONAL=set((
  "bitcoin", "chrome", "cvs", "feed", "git", "irc", "magnet",
  "sftp", "smtp", "ssh", "steam", "svn"
))

URI_SCHEMES = URI_SCHEMES_PERMANENT.union(URI_SCHEMES_PROVISIONAL)

def isInt(v):
   return v is not True and v is not False and isinstance(v, int)

# SQUISH_MIME_RULES = (
#    # IMPORTANT: Order matters a great deal here! Full mime-types should come
#    #         first, with the shortest codes preceding the longer ones.
#    ('text/plain', 'tp/'),
#    ('text/html', 'h/'),
#    ('application/zip', 'z/'),
#    ('application/json', 'j/'),
#    ('application/pdf', 'p/'),
#    ('application/rtf', 'r/'),
#    ('application/octet-stream', 'o/'),
#    ('application/msword', 'ms/d'),
#    ('application/vnd.ms-excel', 'ms/x'),
#    ('application/vnd.ms-access', 'ms/m'),
#    ('application/vnd.ms-powerpoint', 'ms/p'),
#    ('application/pgp-keys', 'pgp/k'),
#    ('application/pgp-signature', 'pgp/s'),
#    ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'ms/xx'),
#    ('application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'ms/dx'),
#    ('application/vnd.openxmlformats-officedocument.presentationml.presentation', 'ms/px'),
#    # These are prefixes that apply to many document types
#    ('application/vnd.openxmlformats-officedocument.', 'msx/'),
#    ('application/vnd.', 'vnd/'),
#    ('application/x-', 'x/'),
#    ('application/', '/'),
#    ('video/', 'v/'),
#    ('audio/', 'a/'),
#    ('image/', 'i/'),
#    ('text/', 't/'))


# def squish_mimetype(mimetype):
#    for prefix, rep in SQUISH_MIME_RULES:
#       if mimetype.startswith(prefix):
#          return rep + mimetype[len(prefix):]
#    return mimetype


# def unsquish_mimetype(mimetype):
#    for prefix, rep in reversed(SQUISH_MIME_RULES):
#       if mimetype.startswith(rep):
#          return prefix + mimetype[len(rep):]
#    return mimetype

class RepairDialogLinking(object):
   problemName='Parent message missed'
   def __init__(self, store):
      self.__msg_progress=consoleColor['clearLast']+'%i (%i missed) from %i'
      self.store=store

   def count(self, user):
      ids=(self.store.userId(user), 'node_problem', self.store.problemId(self.problemName))
      return self.store.db.countBacklinks(ids)

   def find_broken_msgs_without_dialogs(self, user):
      tArr=[]
      g=self.store.db.iterBranch((self.store.userId(user), 'node_date'), strictMode=True, recursive=True, treeMode=True, safeMode=False, calcProperties=False, skipLinkChecking=True)
      for ids, (props, l) in g:
         if len(ids)<4: continue
         if ids[3]!='node_msg': g.send(False)  # skip not-msgs nodes
         if len(ids)>5: g.send(False)  # skip branch inside msgs
         if len(ids)==5:
            try:
               self.store.dialogFind_byMsgIds(ids, strictMode=True, asThread=True)
            except Exception:
               tArr.append(ids)
      return tArr

   def run(self, user):
      userId=self.store.userId(user)
      problemId=self.store.problemId(self.problemName)
      ids=(userId, 'node_problem', problemId)
      c=self.store.db.countBacklinks(ids)
      if not c: return
      parser=ImportMail_MBox(None)
      i1=i2=0
      if IS_TTY: print
      for idsCur, (propsCur, lCur) in self.store.db.iterBacklinks(ids, recursive=False, allowContextSwitch=False):
         msgIds=idsCur[:-1]
         msgId=msgIds[-1]
         dateId=idsCur[-4]
         idsFrom=self.store.dialogFind_byMsg(userId, msgId, date=dateId, asThread=True)
         oldDialog=(userId, 'node_dialog', self.store.dialogId(self.store._idsConv_thread2dialog(idsFrom, onlyDialog=True)))
         data=self.store.msgGet(userId, msgId, date=dateId, strictMode=True, onlyPublic=False, resolveAttachments=False, andLabels=False)
         raw=self.store._fileGet('raw', data['_raw'])
         headers=parser._parseHeaders(parser._prepMsgObj(raw))
         replyPoint=self.store._extract_replyPoint(headers)
         idsTo=self.store.dialogFind_byMsg(userId, replyPoint, asThread=True)
         if idsTo:
            self.store.db.move(idsFrom, idsTo+(msgId,), onlyIfExist=True, strictMode=True, fixLinks=True, recursive=True)
            self.store.db.remove(oldDialog)
            self.store.db.remove(idsCur)
            i1+=1
         else: i2+=1
         print self.__msg_progress%(i1, i2, c)
