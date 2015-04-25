var Drill = Drill || {};

Drill.Site = {
  init : function(){
    Drill.Site.watchExpandMenuClicks();
    Drill.Site.watchInternalAnchorClicks();
    Drill.Site.watchSearchBarMouseEnter();
    Drill.Site.watchSearchSubmit();
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
    var item_to_check = $($("#menu ul li")[3]).css("display");
    return (item_to_check != 'none');
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

  watchSearchSubmit: function() {
    $("#menu .search-bar #drill-search-form").on("submit", function(e){
      e.preventDefault();
      var search_val = $("#drill-search-term").val();
      var search_url = "https://www.google.com/webhp?ie=UTF-8#q="+search_val+"%20site%3Adrill.apache.org%20OR%20site%3Aissues.apache.org%2Fjira%2Fbrowse%2FDRILL%20OR%20site%3Amail-archives.apache.org%2Fmod_mbox%2Fdrill-user";
      var form = $("#menu .search-bar form#search-using-google");
      form.attr("action",search_url);
      form.submit();
      form.attr("action","");
    });
  },

  watchInternalAnchorClicks : function() {
    $("a.anchor").css({ display: "inline" });
    Drill.Site.offsetHeader();
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
    Drill.Docs.add_expand_contract_buttons();
    Drill.Docs.show_or_hide_on_click();
    Drill.Docs.setCurrentDoc();
    Drill.Docs.watchCategoryBar();
    Drill.Docs.watchDocTocClicks();
    Drill.Docs.watchExpandTocClicks();
    Drill.Docs.permalinkSubHeaders();
  },

  setCurrentDoc : function() {
    var current_l1 = $("li.toctree-l3.current");
    var current_l2 = $("li.toctree-l2.current");
    var current_l3 = $("li.toctree-l3.current");

    Drill.Docs._setPlusMinus(current_l2);
    Drill.Docs._setPlusMinus( current_l3.parent().prev("li") ); // requires knowledge of html structure...bad...bad.
    // alternatively, set these up in the rendering of the docs, like the current_section and current are done.

    // set current <li>, starting from innermost possibility
    if(current_l3.length > 0){
      current_l3.addClass("current");
    } else if(current_l2.length > 0){
      current_l2.addClass("current");
    } else if(current_l1.length > 0){
      current_l1.addClass("current");
    }
  },

  _setPlusMinus : function(parent) {
    parent.children("span.expand").removeClass('show');
    parent.children("span.contract").addClass('show');
  },

  watchCategoryBar : function() {
    $(window).scroll(function(){
      var category_bar = $(".toc-categories");
      if ($(this).scrollTop() > 35) {
        category_bar.addClass('fixed');
        $(".page-wrap div.int_title").addClass("margin_110");
      } else {
        category_bar.removeClass('fixed');
        $(".page-wrap div.int_title").removeClass("margin_110");
      }
    });
  },

  watchExpandTocClicks : function () {
    $(".expand-toc-icon").on("click", function(){
      if($(".int_text .sidebar").css('left') == '0px'){
        Drill.Docs._contractSidebar();
      } else {
        Drill.Docs._expandSidebar();
      }
    })
  },

  watchDocTocClicks : function(){
    $('li.toctree-l1').on('click', function(){
      Drill.Docs._make_current(this);
    })
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
            $toctree.children("span.expand").removeClass('show');
            $toctree.children("span.contract").addClass('show');
          }

          $(this).slideDown();
        } else {

          if ( $.inArray( $toctree[0], l2nodes ) > -1 ) {
            $toctree.children("span.expand").addClass('show');
            $toctree.children("span.contract").removeClass('show');
          }

          $(this).slideUp();
        }
      })
    })
  },


  add_expand_contract_buttons : function() {
    var expand_btn = '<span class="expand show"><i class="fa fa-plus"></i></span>';
    var contract_btn = '<span class="contract"><i class="fa fa-minus"></i></span>';
    $('li.toctree-l2').filter(function(){ 
      return $(this).next('ul').length > 0;
    })
    .prepend(expand_btn + contract_btn);
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
  },

  _expandSidebar : function(){
    $(".int_text .sidebar").addClass("force-expand");
  },

  _contractSidebar : function() {
    $(".int_text .sidebar").removeClass("force-expand");
  },

  _make_current : function(that) {
    Drill.Docs._remove_current();
    $(that).addClass("current_section");
    $(that).next('ul').addClass("current_section");
  },

  _remove_current : function() {
    $(".current_section").removeClass("current_section");
  },

  _l2nodes_with_children : function(){
    $('li.toctree-l2').filter(function(){ 
      return $(this).next('ul').length > 0;
    });
  }
}

$(function(){
  Drill.Docs.init();
  Drill.Site.init();
});
