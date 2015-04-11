var Drill = Drill || {};

Drill.Site = {
  init : function(){
    Drill.Site.watchExpandMenuClicks();
    Drill.Site.watchInternalAnchorClicks();
    Drill.Site.watchSearchBarMouseEnter();
  },

  watchExpandMenuClicks : function(){
    $("#menu ul li.expand-menu a").on("click", function(){
       if( Drill.Site.menuIsExpanded() ){
        Drill.Site.contractMenu();
      } else {
        Drill.Site.expandMenu();
      }
    })
  },

  menuIsExpanded : function() {
    return ($("#menu ul li.d").css('display') == 'block');
  },

  expandMenu: function(){
    $("#menu ul li").addClass("force-expand");
  },

  contractMenu: function() {
    $("#menu ul li").removeClass("force-expand");
  },

  watchSearchBarMouseEnter: function() {
    $("#menu .search-bar input[type=text]").on({
      focus: function(){
        $(this).animate({ width: '125px' });
      },
      blur: function() {
        $(this).animate({ width: '44px' });
      }
    })
  },

  watchInternalAnchorClicks : function() {
    $("a.anchor").css({ display: "inline" });
    Drill.Site.offsetHeader();
  },

  pathname : function(loc) {
    return (loc.protocol + '//' + loc.host + loc.pathname)
  },

  offsetHeader : function() {
    if (location.hash) {
      var hash = location.hash.replace("#","");
      var aOffset = $('a[name='+hash+']').offset();
      if (typeof aOffset !== 'undefined'){
        $('html, body').animate({
             'scrollTop': aOffset.top
          }, 500);
      }

      // Offset page by the fixed menu's height when an internal anchor is present, i.e. /docs/json-data-model/#flatten-arrays
      var idOffset = $('#'+hash).offset();
      var fixedMenuHeight = $("#menu").height();
      if (typeof idOffset !== 'undefined'){
        $('html, body').animate({
             'scrollTop': idOffset.top - fixedMenuHeight
          }, 500);
      }
    }
  },

  copyToClipboard : function(text) {
    return window.prompt("Copy permalink to clipboard:  + c (windows: Ctrl + c), Enter", text);
  }

}

Drill.Docs = {
  init : function(){
    Drill.Docs.watchDocTocClicks();
    Drill.Docs.watchExpandTocClicks();
    Drill.Docs.permalinkSubHeaders();
  },

  watchExpandTocClicks : function () {
    $(".expand-toc-icon").on("click", function(){
      if($(".int_text .sidebar").css('left') == '0px'){
        Drill.Docs.contractSidebar();
      } else {
        Drill.Docs.expandSidebar();
      }
    })
  },

  expandSidebar : function(){
    $(".int_text .sidebar").addClass("force-expand");
  },

  contractSidebar : function() {
    $(".int_text .sidebar").removeClass("force-expand");
  },

  l2nodes_with_children : function(){
    $('li.toctree-l2').filter(function(){ 
      return $(this).next('ul').length > 0;
    });
  },

  watchDocTocClicks : function(){
    $('li.toctree-l1').on('click', function(){
      Drill.Docs.make_current(this);
    })

    Drill.Docs.add_expand_contract_buttons();
    Drill.Docs.show_or_hide_on_click();
  },

  show_or_hide_on_click : function() {
    var l2nodes = $('li.toctree-l2').filter(function(){ 
      return $(this).next('ul').length > 0;
    });
    $('li[class^=toctree]')
    .filter(function(){
      return $(this).next('ul').length > 0;
    })
    .on("click", function(){
      var $toctree = $(this);
      var $this_ul = $toctree.next("ul");
      $.each( $this_ul, function(i){
        if ( $(this).is(':hidden') ) {

          if ( $.inArray( $toctree[0], l2nodes ) > -1 ) {
            $toctree.children("span.expand").hide();
            $toctree.children("span.contract").show();
          }

          $(this).show("slide");
        } else {

          if ( $.inArray( $toctree[0], l2nodes ) > -1 ) {
            $toctree.children("span.expand").show();
            $toctree.children("span.contract").hide();
          }

          $(this).hide("slide");
        }
        //$(this).toggle("slide");
      })

      $('li[class^=toctree] ul').not($this_ul).hide("slide");
    })
  },

  make_current : function(that) {
    Drill.Docs.remove_current();
    $(that).addClass("current");
    $(that).next('ul').addClass("current");
  },

  remove_current : function() {
    $(".current").removeClass("current");
  },

  add_expand_contract_buttons : function() {
    $('li.toctree-l2').filter(function(){ 
      return $(this).next('ul').length > 0;
    })
    .prepend('<span class="expand"><i class="fa fa-plus"></i></span><span class="contract"><i class="fa fa-minus"></i></span>');
  },

  permalinkSubHeaders : function() {
    var subheaders = $.merge($(".main-content h2[id]"), $(".main-content h3[id]"));
    $.each( subheaders, function( index, el ){
      //create permalink element
      var permalink = "<a class='hidden permalink' href='javascript:void(0);' title='Grab the permalink!'> ¶</a>";
      $(el).append(permalink);

      //show permalink element on hover
      $(el).on({
        mouseenter: function(){
          $(this).children("a.permalink").show();
        },
        mouseleave: function() {
          $(this).children("a.permalink").hide();
        }
      });
    })

    $(".main-content .permalink").on("click", function(){
      var hash = $(this).parent().attr('id');
      window.location.hash = hash;
      Drill.Site.offsetHeader();
      //Drill.Site.copyToClipboard(Drill.Site.pathname(location) + "#" + hash);
    })
  }
}

$(function(){
  Drill.Docs.init();
  Drill.Site.init();
});
