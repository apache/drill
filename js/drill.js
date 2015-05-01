---
---
var Drill = Drill || {};

Drill.Site = {
  init : function(){
    Drill.Site.watchExpandMenuClicks();
    Drill.Site.watchInternalAnchorClicks();
    Drill.Site.watchSearchBarMouseEnter();
    Drill.Site.watchSearchSubmit();
    Drill.Site.setSearchVal();
  },
  
  getParameterByName : function(name) {
      name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
      var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
          results = regex.exec(location.search);
      return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
  },
  
  setSearchVal : function() {
    $("#drill-search-term").val(Drill.Site.getParameterByName('q'));
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
    // Note: depends on status of documentation-menu item to check menu visibility. May be a cleaner approach.
    var item_to_check = $($("#menu ul li.documentation-menu")).css("display");
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
      var search_url = "{{ site.baseurl }}/search/?q="+search_val;
      document.location.href = search_url;
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
    Drill.Docs.watchDocTocClicks();
    Drill.Docs.watchExpandTocClicks();
    Drill.Docs.permalinkSubHeaders();
  },

  setCurrentDoc : function() {
    var current_l2 = $("li.toctree-l2.current");
    var current_l3 = $("li.toctree-l3.current");

    Drill.Docs._setPlusMinus(current_l2);
    Drill.Docs._setPlusMinus( current_l3.parent().prev("li") ); // requires knowledge of html structure...bad...bad.
    // alternatively, set these up in the rendering of the docs, like the current_section and current are done.
  },

  _setPlusMinus : function(parent) {
    parent.children("span.expand").removeClass('show');
    parent.children("span.contract").addClass('show');
  },

  watchExpandTocClicks : function () {
    $(".expand-toc-icon").on("click", function(){
      //  This relies on .sidebar's 'left' attribute...may be a cleaner approach
      if($(".sidebar").css('left') == '0px'){
        Drill.Docs._contractSidebar();
        //Drill.Docs._contractView();
      } else {
        Drill.Docs._expandSidebar();
        //Drill.Docs._expandView();
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
      var permalink = "<a class='hide permalink' href='javascript:void(0);' title='Grab the permalink!'> ¶</a>";
      $(el).append(permalink);

      //show permalink element on hover
      $(el).on({
        mouseenter: function(){
          $(this).children("a.permalink").removeClass('hide');
        },
        mouseleave: function() {
          $(this).children("a.permalink").addClass('hide');
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
    $(".sidebar").addClass("force-expand");
  },

  _expandView : function(){
    $("nav.breadcrumbs li:first").addClass("force-expand");
    $(".main-content").addClass("force-expand");
  },

  _contractSidebar : function() {
    $(".sidebar").removeClass("force-expand");
  },

  _contractView : function() {
    $("nav.breadcrumbs li:first").removeClass("force-expand");
    $(".main-content").removeClass("force-expand");
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
