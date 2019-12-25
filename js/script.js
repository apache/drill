---
---
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

	$("#header .dots .dot:eq(0)").addClass("sel");

	resized();

	$(window).scroll(onScroll);

    var pathname = window.location.pathname;
    var pathSlashesReplaced = pathname.replace(/\//g, " ");
    var pathSlashesReplacedNoFirstDash = pathSlashesReplaced.replace(" ","");
    var newClass = pathSlashesReplacedNoFirstDash.replace(/(\.[\s\S]+)/ig, "");
	$("body").addClass(newClass);
    if ( $("body").attr("class") == "")
    {
         $("body").addClass("class");
    }
});

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

	if (WW < 999) $("#menu").addClass("r");
	else $("#menu").removeClass("r");

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
	//if ($("#menu.r").length) {
	//	$("#menu.r").css({ top: ST+"px" });
	//} else {
	//	$("#menu").css({ top: "0px" });
	//}

	if (ST > 400) $("#subhead").addClass("show");
	else $("#subhead").removeClass("show");
}
