# -*- coding: utf-8 -*-
import sgmllib, re, htmlentitydefs
import urllib2, urllib # подключили библиотеку urllib

#### UNICODE #######################################################################################
def decode_utf8(string):
    """ Returns the given string as a unicode string (if possible).
    """
    if isinstance(string, str):
        for encoding in (("utf-8",), ("windows-1252",), ("utf-8", "ignore")):
            try:
                return string.decode(*encoding)
            except:
                pass
        return string
    return unicode(string)

def encode_utf8(string):
    """ Returns the given string as a Python byte string (if possible).
    """
    if isinstance(string, unicode):
        try:
            return string.encode("utf-8")
        except:
            return string
    return str(string)

u = decode_utf8
s = encode_utf8

#### PLAIN TEXT ####################################################################################
BLOCK = [
   "title", "h1", "h2", "h3", "h4", "h5", "h6", "p",
   "center", "blockquote", "div", "table", "ul", "ol", "pre", "code", "form"
]

SELF_CLOSING = ["br", "hr", "img"]

# Element tag replacements for a stripped version of HTML source with strip_tags().
# Block-level elements are followed by linebreaks,
# list items are preceded by an asterisk ("*").
LIST_ITEM = "*"
blocks = dict.fromkeys(BLOCK+["br", "tr", "td"], ("", "\n\n"))
blocks.update({
   "li": ("%s " % LIST_ITEM, "\n"),
   "img": ("", ""),
   "br": ("", "\n"),
   "th": ("", "\n"),
   "tr": ("", "\n"),
   "td": ("", "\t"),
})

class HTMLParser(sgmllib.SGMLParser):

   def __init__(self):
      sgmllib.SGMLParser.__init__(self)

   def handle_starttag(self, tag, attrs):
      pass

   def handle_endtag(self, tag):
      pass

   def unknown_starttag(self, tag, attrs):
      self.handle_starttag(tag, attrs)

   def unknown_endtag(self, tag):
      self.handle_endtag(tag)

   def clean(self, html):
      html = decode_utf8(html)
      html = html.replace("/>", " />")
      html = html.replace("  />", " />")
      html = html.replace("<!", "&lt;!")
      html = html.replace("&lt;!DOCTYPE", "<!DOCTYPE")
      html = html.replace("&lt;!doctype", "<!doctype")
      html = html.replace("&lt;!--", "<!--")
      return html


   def parse_declaration(self, i):
      # We can live without sgmllib's parse_declaration().
      try:
         return sgmllib.SGMLParser.parse_declaration(self, i)
      except sgmllib.SGMLParseError:
         return i + 1

   def convert_charref(self, name):
      # This fixes a bug in older versions of sgmllib when working with Unicode.
      # Fix: ASCII ends at 127, not 255
      try:
         n = int(name)
      except ValueError:
         return
      if not 0 <= n <= 127:
         return
      return chr(n)

class HTMLTagstripper(HTMLParser):

   def __init__(self):
      HTMLParser.__init__(self)

   def strip(self, html, exclude=[], replace=blocks):
      """ Returns the HTML string with all element tags (e.g. <p>) removed.
         - exclude   : a list of tags to keep. Element attributes are stripped.
                     To preserve attributes a dict of (tag name, [attribute])-items can be given.
         - replace   : a dictionary of (tag name, (replace_before, replace_after))-items.
                     By default, block-level elements are separated with linebreaks.
      """
      if html is None:
         return None
      self._exclude = isinstance(exclude, dict) and exclude or dict.fromkeys(exclude, [])
      self._replace = replace
      self._data   = []
      self.feed(self.clean(html))
      self.close()
      self.reset()
      return "".join(self._data)

   def handle_starttag(self, tag, attributes):
      if tag in self._exclude:
         # Create the tag attribute string,
         # including attributes defined in the HTMLTagStripper._exclude dict.
         a = len(self._exclude[tag]) > 0 and attributes or []
         a = ["%s=\"%s\"" % (k,v) for k, v in a if k in self._exclude[tag]]
         a = (" "+" ".join(a)).rstrip()
         self._data.append("<%s%s>" % (tag, a))
      if tag in self._replace:
         self._data.append(self._replace[tag][0])
      if tag in self._replace and tag in SELF_CLOSING:
         self._data.append(self._replace[tag][1])

   def handle_endtag(self, tag):
      if tag in self._exclude and self._data and self._data[-1].startswith("<"+tag):
         # Never keep empty elements (e.g. <a></a>).
         self._data.pop(-1); return
      if tag in self._exclude:
         self._data.append("</%s>" % tag)
      if tag in self._replace:
         self._data.append(self._replace[tag][1])

   def handle_data(self, data):
      self._data.append(data.strip("\n\t"))
   def handle_entityref(self, ref):
      self._data.append("&%s;" % ref)
   def handle_charref(self, ref):
      self._data.append("&%s;" % ref)

   def handle_comment(self, comment):
      if "comment" in self._exclude or \
            "!--" in self._exclude:
         self._data.append("<!--%s-->" % comment)

# As a function:
strip_tags = HTMLTagstripper().strip

def strip_element(string, tag, attributes=""):
   """ Removes all elements with the given tagname and attributes from the string.
      Open and close tags are kept in balance.
      No HTML parser is used: strip_element(s, "a", "href='foo' class='bar'")
      matches "<a href='foo' class='bar'" but not "<a class='bar' href='foo'".
   """
   s = string.lower() # Case-insensitive.
   t = tag.strip("</>")
   a = (" " + attributes.lower().strip()).rstrip()
   i = 0
   j = 0
   while j >= 0:
      i = s.find("<%s%s" % (t, a), i)
      j = s.find("</%s>" % t, i+1)
      opened, closed = s[i:j].count("<%s" % t), 1
      while opened > closed and j >= 0:
         k = s.find("</%s>" % t, j+1)
         opened += s[j:k].count("<%s" % t)
         closed += 1
         j = k
      if i < 0: return string
      if j < 0: return string[:i]
      string = string[:i] + string[j+len(t)+3:]; s=string.lower()
   return string

def strip_between(a, b, string):
   """ Removes anything between (and including) string a and b inside the given string.
   """
   p = "%s.*?%s" % (a, b)
   p = re.compile(p, re.DOTALL | re.I)
   return re.sub(p, "", string)

def strip_javascript(html):
   return strip_between("<script.*?>", "</script>", html)
def strip_inline_css(html):
   return strip_between("<style.*?>", "</style>", html)
def strip_comments(html):
   return strip_between("<!--", "-->", html)
def strip_forms(html):
   return strip_between("<form.*?>", "</form>", html)

RE_AMPERSAND = re.compile("\&(?!\#)")         # & not followed by #
RE_UNICODE   = re.compile(r'&(#?)(x|X?)(\w+);') # &#201;

def encode_entities(string):
   """ Encodes HTML entities in the given string ("<" => "&lt;").
      For example, to display "<em>hello</em>" in a browser,
      we need to pass "&lt;em&gt;hello&lt;/em&gt;" (otherwise "hello" in italic is displayed).
   """
   if isinstance(string, (str, unicode)):
      string = RE_AMPERSAND.sub("&amp;", string)
      string = string.replace("<", "&lt;")
      string = string.replace(">", "&gt;")
      string = string.replace('"', "&quot;")
      string = string.replace("'", "&#39;")
   return string

def decode_entities(string):
   """ Decodes HTML entities in the given string ("&lt;" => "<").
   """
   # http://snippets.dzone.com/posts/show/4569
   def replace_entity(match):
      hash, hex, name = match.group(1), match.group(2), match.group(3)
      if hash == "#" or name.isdigit():
         if hex == '' :
            return unichr(int(name))             # "&#38;" => "&"
         if hex in ("x","X"):
            return unichr(int('0x'+name, 16))      # "&#x0026;" = > "&"
      else:
         cp = htmlentitydefs.name2codepoint.get(name) # "&amp;" => "&"
         # cp=name
         return cp and unichr(cp) or match.group()   # "&foo;" => "&foo;"
   if isinstance(string, (str, unicode)):
      return RE_UNICODE.subn(replace_entity, string)[0]
   return string

def decode_url(string):
   return urllib.quote_plus(string)
def encode_url(string):
   return urllib.unquote_plus(string) # "black/white" => "black%2Fwhite".

RE_SPACES = re.compile("( |\xa0)+", re.M) # Matches one or more spaces.
RE_TABS   = re.compile(r"\t+", re.M)     # Matches one or more tabs.

def collapse_spaces(string, indentation=False, replace=" "):
   """ Returns a string with consecutive spaces collapsed to a single space.
      Whitespace on empty lines and at the end of each line is removed.
      With indentation=True, retains leading whitespace on each line.
   """
   p = []
   for x in string.splitlines():
      n = indentation and len(x) - len(x.lstrip()) or 0
      p.append(x[:n] + RE_SPACES.sub(replace, x[n:]).strip())
   return "\n".join(p)

def collapse_tabs(string, indentation=False, replace=" "):
   """ Returns a string with (consecutive) tabs replaced by a single space.
      Whitespace on empty lines and at the end of each line is removed.
      With indentation=True, retains leading whitespace on each line.
   """
   p = []
   for x in string.splitlines():
      n = indentation and len(x) - len(x.lstrip()) or 0
      p.append(x[:n] + RE_TABS.sub(replace, x[n:]).strip())
   return "\n".join(p)

def collapse_linebreaks(string, threshold=1):
   """ Returns a string with consecutive linebreaks collapsed to at most the given threshold.
      Whitespace on empty lines and at the end of each line is removed.
   """
   n = "\n" * threshold
   p = [s.rstrip() for s in string.splitlines()]
   string = "\n".join(p)
   string = re.sub(n+r"+", n, string)
   return string

def plaintext(html, keep=[], replace=blocks, linebreaks=2, indentation=False):
   """ Returns a string with all HTML tags removed.
      Content inside HTML comments, the <style> tag and the <script> tags is removed.
      - keep      : a list of tags to keep. Element attributes are stripped.
                  To preserve attributes a dict of (tag name, [attribute])-items can be given.
      - replace    : a dictionary of (tag name, (replace_before, replace_after))-items.
                  By default, block-level elements are followed by linebreaks.
      - linebreaks  : the maximum amount of consecutive linebreaks,
      - indentation : keep left line indentation (tabs and spaces)?
   """
   if not keep.__contains__("script"):
      html = strip_javascript(html)
   if not keep.__contains__("style"):
      html = strip_inline_css(html)
   if not keep.__contains__("form"):
      html = strip_forms(html)
   if not keep.__contains__("comment") and \
      not keep.__contains__("!--"):
      html = strip_comments(html)
   html = html.replace("\r", "\n")
   html = strip_tags(html, exclude=keep, replace=replace)
   html = decode_entities(html)
   html = collapse_spaces(html, indentation)
   html = collapse_tabs(html, indentation)
   html = collapse_linebreaks(html, linebreaks)
   html = html.strip()
   return html

if(__name__=='__main__'):
   print 'ok'
   raw_input()
