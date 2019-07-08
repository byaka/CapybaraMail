function maybeScrollToHash() {
    if (window.location.hash && $(window.location.hash).length) {
        var newTop = $(window.location.hash).offset().top - $('.navbar-fixed-top').height() + parseInt($('body').css('padding-top'),10);
        $(window).scrollTop(newTop);
    }
}

$(window).bind('hashchange', function() {
  maybeScrollToHash();
});

maybeScrollToHash();