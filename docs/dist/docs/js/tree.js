$(function () {
   $('.tree.collapsable li').on('click', function (e) {
      var children=$(this).find('> ul');
      if (children.hasClass('collapsed')) children.removeClass('collapsed');
      else children.addClass('collapsed');
      e.stopPropagation();
   });
});

// $(function () {
//    $('.tree li').hide();
//    $('.tree li:first').show();
//    $('.tree li').on('click', function (e) {
//       var children=$(this).find('> ul > li');
//       if (children.is(":visible")) children.hide('fast');
//       else children.show('fast');
//       e.stopPropagation();
//    });
// });