/*
  Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. */


$(window).load(function () {
    document.getElementById("renderbutton").addEventListener("click", function () {
	this.remove();
	var queryid = window.location.href.split("/").splice(-1);
	$.ajax({
	    type: "GET",
	    dataType: "json",
	    url: "/profiles/" + queryid + ".json",
	    success: function (profile) {
		var colors = ["red", "green", "blue", "orange", "yellow", "pink", "purple", "gray"];

		var plan = $.map(profile.plan.trim().split("\n"), function (s) {
		    return [/^([0-9-]+)( *)([a-zA-Z]*)/.exec(s).slice(1)];
		});

		// nodes
		var g = new dagreD3.Digraph();
		for (var i = 0; i < plan.length; i++) {
		    g.addNode(plan[i][0], {
			label: plan[i][2],
			fragment: parseInt(plan[i][0].split("-")[0])
		    });
		}

		// edges
		var st = [plan[0]];
		for (var i = 1; i < plan.length; i++) {
		    var top = st.pop();
		    while (top[1].length >= plan[i][1].length)
			top = st.pop();

		    g.addEdge(null, plan[i][0], top[0]);

		    if (plan[i][1].length != top[1].length)
			st.push(top);
		    if (plan[i][1].length >= top[1].length)
			st.push(plan[i]);
		}

		// rendering
		var renderer = new dagreD3.Renderer();
		renderer.zoom(function () {return function (graph, root) {}});
		var oldDrawNodes = renderer.drawNodes();
		renderer.drawNodes(function(graph, root) {
		    var svgNodes = oldDrawNodes(graph, root);
		    svgNodes.each(function(u) {
			d3.select(this).style("fill", colors[graph.node(u).fragment % colors.length]);
		    });
		    return svgNodes;
		});

		// page placement
		var layout = dagreD3.layout()
                    .nodeSep(20);//.rankDir("LR");
		var layout = renderer.layout(layout).run(g, d3.select("svg g"));

		//var layout = renderer.run(g, d3.select("svg g"));
		d3.select("svg")
		    .attr("width", layout.graph().width + 40)
		    .attr("height", layout.graph().height + 40);
	    },
	    error: function (x, y, z) {
		console.log(x);
		console.log(y);
		console.log(z);
	    }
	});
    }, false);
});
