// JavaScript Document



var reelPointer = null;
$(document).ready(function(e) {
	
  $(".aLeft").click(function() {
		moveReel("prev");
	});
	$(".aRight").click(function() {
		moveReel("next");
	});
	
	if ($("#header .scroller .item").length == 1) {
		
	} else {
		
		$("#header .dots, .aLeft, .aRight").css({ display: 'block' });
		$("#header .scroller .item").each(function(i) {
			$("#header .dots").append("<div class='dot'></div>");
			$("#header .dots .dot").eq(i).click(function() {
				var index = $(this).prevAll(".dot").length;
				moveReel(index);
			});
		});
		
		reelPointer = setTimeout(function() { moveReel(1); },5000);
	}
	
	$("#menu ul li").each(function(index, element) {
        if ($(this).find("ul").length) {
			$(this).addClass("parent");	
		}
    });
	
	$("#menu ul li.parent").mouseenter(function() {
		closeSearch();
	});
	
	$("#header .dots .dot:eq(0)").addClass("sel");
	
	$("#menu ul li.l").click(function() {
		if ($(this).hasClass("open")) {
			//devo chiudere	
			closeSearch();
		} else {
			//devo aprire
			$("#search").css({ display: "block", paddingTop: "0px", paddingBottom: "0px", marginTop:"-40px" }).animate({ paddingTop: "30px", paddingBottom: "30px", marginTop: "0px" },400,"easeOutQuint");
			$("#search input").trigger("focus").select();
			$(this).addClass("open");
		}
	});
	
	$("#search input").val("").keypress(function(e) {
		if(e.keyCode == 13 && $(this).val()) {
			document.location.href = "https://www.google.com/webhp?ie=UTF-8#q="+$(this).val()+"%20site%3Aincubator.apache.org%2Fdrill%20OR%20site%3Aissues.apache.org%2Fjira%2Fbrowse%2FDRILL%20OR%20site%3Amail-archives.apache.org%2Fmod_mbox%2Fincubator-drill-dev";
		}
	});	
    
	resized();
	
	$(window).scroll(onScroll);
});

function closeSearch() {
	var R = ($("#menu ul li.l.open").length) ? true : false;
	$("#menu ul li.l").removeClass("open");
	$("#search").stop(false,true,false).animate({ paddingTop: "0px", paddingBottom: "0px", marginTop: "-40px" },400,"easeInQuint",function() {
		$(this).css({ display: "none" });	
	});
	return R;
}

var reel_currentIndex = 0;
function resized() {
	
	var WW = parseInt($(window).width(),10);
	var IW = (WW < 999) ? 999 : WW;
	var IH = parseInt($("#header .scroller .item").css("height"),10);
	var IN = $("#header .scroller .item").length;
	
	$("#header .scroller").css({ width: (IN * IW)+"px", marginLeft: -(reel_currentIndex * IW)+"px" });
	$("#header .scroller .item").css({ width: IW+"px" });
	
	
	$("#header .scroller .item").each(function(i) {
		var th = parseInt($(this).find(".tc").height(),10);
		var d = IH - th + 25;
		$(this).find(".tc").css({ top: Math.round(d/2)+"px" });
	});
	
	if (WW < 999) $("#menu, #search").addClass("r");
	else $("#menu, #search").removeClass("r");
	
	onScroll();
		
}

function moveReel(direction) {
	
	if (reelPointer) clearTimeout(reelPointer);
	
	var IN = $("#header .scroller .item").length;
	var IW = $("#header .scroller .item").width();
	if (direction == "next") reel_currentIndex++;
	else if (direction == "prev") reel_currentIndex--;
	else reel_currentIndex = direction;
	
	if (reel_currentIndex >= IN) reel_currentIndex = 0;
	if (reel_currentIndex < 0) reel_currentIndex = IN-1;
	
	$("#header .dots .dot").removeClass("sel");
	$("#header .dots .dot").eq(reel_currentIndex).addClass("sel");
		
	$("#header .scroller").stop(false,true,false).animate({ marginLeft: -(reel_currentIndex * IW)+"px" }, 1000, "easeOutQuart");
	
	reelPointer = setTimeout(function() { moveReel(1); },5000);
	
}

function onScroll() {
	var ST = document.body.scrollTop || document.documentElement.scrollTop;
	if ($("#menu.r").length) {
		$("#menu.r").css({ top: ST+"px" });	
		$("#search.r").css({ top: (ST+50)+"px" });	
	} else {
		$("#menu").css({ top: "0px" });
		$("#search").css({ top: "50px" });
	}
	
	if (ST > 400) $("#subhead").addClass("show");	
	else $("#subhead").removeClass("show");	
}