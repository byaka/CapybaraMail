# -*- coding: utf-8 -*-

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
      self.store=store

   def count(self, user):
      ids=(self.store.userId(user), 'node_problem', self.store.problemId(self.problemName))
      return self.store.db.countBacklinks(ids)

   def run(self, user):
      userId=self.store.userId(user)
      problemId=self.store.problemId(self.problemName)
      ids=(userId, 'node_problem', problemId)
      if not self.store.db.countBacklinks(ids): return
      for idsCur, (propsCur, lCur) in self.store.db.iterBacklinks(ids, recursive=False, allowContextSwitch=False):
         pass


