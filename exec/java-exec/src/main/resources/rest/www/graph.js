/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership. The ASF licenses this file to
 *  You under the Apache License, Version 2.0 (the "License"); you may not use
 *  this file except in compliance with the License. You may obtain a copy of
 *  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
 *  by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 *  OF ANY KIND, either express or implied. See the License for the specific
 *  language governing permissions and limitations under the License.
 */

$(window).load(function () {
    // for each record, unroll the array pointed to by "fieldpath" into a new
    // record for each element of the array
    function unnest (table, fieldpath, dest) {
        var faccess = accessor(fieldpath);
        return $.map(table, function (record, index) {
            var ra = [];
            var nested = faccess(record);
            for (var i = 0; i < nested.length; i++) {
                var newrec = $.extend({}, record);
                newrec[dest] = nested[i];
                ra.push(newrec);
            }
            return ra;
        });
    }

    // for each record, project "fieldpath" into "dest".
    function extract (table, fieldpath, dest) {
        var faccess = accessor(fieldpath);
        return $.map(table, function (record, index) {
            var newrec = $.extend({}, record);
            newrec[dest] = faccess(newrec);
            return newrec;
        });
    }

    // creates a function that will traverse tree of objects based the '.'
    // delimited "path"
    function accessor (path) {
        path = path.split(".");
        return function (obj) {
            for (var i = 0; i < path.length; i++)
                obj = obj[path[i]];
            return obj;
        }
    }

    // sample use of unnest/extract
    function extractminortimes (profile) {
        var t1 = unnest([profile], "fragmentProfile", "ma");
        var t2 = unnest(t1, "ma.minorFragmentProfile", "mi");

        var timetable = $.map(t2, function (record, index) {
            var newrec = {
                "name" : record.ma.majorFragmentId + "-" +
                    record.mi.minorFragmentId,
                "category" : record.ma.majorFragmentId,
                "start" : (record.mi.startTime - record.start) / 1000.0,
                "end" : (record.mi.endTime - record.start) / 1000.0
            };
            return newrec;
        });

        timetable.sort(function (r1, r2) {
            if (r1.category == r2.category) {
                //return r1.name > r2.name;
                return r1.end - r1.start > r2.end - r2.start ? 1 : -1;
            }
            else return r1.category > r2.category ? 1 : -1;

        });
        return timetable;
    }

    // write the "fieldpaths" for the table "table" into the "domtable"
    function builddomtable (domtable, table, fieldpaths) {
        var faccessors = $.map(fieldpaths, function (d, i) {
            return accessor(d);
        });

        var domrow = domtable.append("tr");
        for (var i = 0; i < fieldpaths.length; i++)
            domrow.append("th").text(fieldpaths[i]);
        for (var i = 0; i < table.length; i++) {
            domrow = domtable.append("tr");
            for (var j = 0; j < faccessors.length; j++)
                domrow.append("td").text(faccessors[j](table[i]));
        }
    }

    // parse the short physical plan into a dagreeD3 structure
    function parseplan (planstring) {
        var g = new dagreD3.Digraph();
        var ps = $.map(planstring.trim().split("\n"), function (s) {
            return [/^([0-9-]+)( *)([a-zA-Z]*)/.exec(s).slice(1)];
        });

        // nodes
        for (var i = 0; i < ps.length; i++) {
            g.addNode(ps[i][0], {
                label: ps[i][2],
                fragment: parseInt(ps[i][0].split("-")[0])
            });
        }

        // edges
        var st = [ps[0]];
        for (var i = 1; i < ps.length; i++) {
            var top = st.pop();
            while (top[1].length >= ps[i][1].length)
                top = st.pop();

            g.addEdge(null, ps[i][0], top[0]);

            if (ps[i][1].length != top[1].length)
                st.push(top);
            if (ps[i][1].length >= top[1].length)
                st.push(ps[i]);
        }
        return g;
    }

    // graph a "planstring" into the d3 svg handle "svg"
    function buildplangraph (svg, planstring) {
        var padding = 20;
        var graph = parseplan(planstring);

        var renderer = new dagreD3.Renderer();
        renderer.zoom(function () {return function (graph, root) {}});

        var oldDrawNodes = renderer.drawNodes();
        renderer.drawNodes(function(graph, root) {
            var svgNodes = oldDrawNodes(graph, root);
            svgNodes.each(function(u) {
                var fc = d3.rgb(globalconfig.majorcolorscale(graph.node(u).fragment));
                d3.select(this).select("rect")
                    .style("fill", graph.node(u).label.endsWith("Exchange") ? "white" : fc)
                    .style("stroke", "#000")
                    .style("stroke-width", "1px")
            });
            return svgNodes;
        });

        var oldDrawEdgePaths = renderer.drawEdgePaths();
        renderer.drawEdgePaths(function(graph, root) {
            var svgEdgePaths = oldDrawEdgePaths(graph, root);
            svgEdgePaths.each(function(u) {
                d3.select(this).select("path")
                    .style("fill", "none")
                    .style("stroke", "#000")
                    .style("stroke-width", "1px")
            });
            return svgEdgePaths;
        });

        var shiftedgroup = svg.append("g")
            .attr("transform", "translate(" + padding + "," + padding + ")");
        var layout = dagreD3.layout().nodeSep(20).rankDir("BT");
        var result = renderer.layout(layout).run(graph, shiftedgroup);

        svg.attr("width", result.graph().width + 2 * padding)
            .attr("height", result.graph().height + 2 * padding);
    }

    function buildtimingchart (svgdest, timetable) {
        var chartprops = {
            "w" : 800,
            "h" : -1,
            "svg" : svgdest,
            "bheight" : 2,
            "bpad" : 0,
            "margin" : 50,
            "scaler" : null,
            "colorer" : null,
        };

        chartprops.h = timetable.length * (chartprops.bheight + chartprops.bpad * 2)

        chartprops.svg
            .attr("width", chartprops.w + 2 * chartprops.margin)
            .attr("height", chartprops.h + 2 * chartprops.margin)
            .attr("class", "svg");

        chartprops.scaler = d3.scale.linear()
            .domain([d3.min(timetable, accessor("start")),
                     d3.max(timetable, accessor("end"))])
            .range([0, chartprops.w - chartprops.bpad * 2]);
        chartprops.colorer = globalconfig.majorcolorscale;

        // backdrop
        chartprops.svg.append("g")
            .selectAll("rect")
            .data(timetable)
            .enter()
            .append("rect")
            .attr("x", 0)
            .attr("y", function(d, i) {return i * (chartprops.bheight + 2 * chartprops.bpad);})
            .attr("width", chartprops.w)
            .attr("height", chartprops.bheight + chartprops.bpad * 2)
            .attr("stroke", "none")
            .attr("fill", function(d) {return d3.rgb(chartprops.colorer(d.category));})
            .attr("opacity", 0.1)
            .attr("transform", "translate(" + chartprops.margin + "," +
                  chartprops.margin + ")");

        // bars
        chartprops.svg.append('g')
            .selectAll("rect")
            .data(timetable)
            .enter()
            .append("rect")
             //.attr("rx", 3)
             //.attr("ry", 3)
            .attr("x", function(d) {return chartprops.scaler(d.start) + chartprops.bpad;})
            .attr("y", function(d, i) {return i * (chartprops.bheight + 2 * chartprops.bpad) + chartprops.bpad;})
            .attr("width", function(d) {return (chartprops.scaler(d.end) - chartprops.scaler(d.start));})
            .attr("height", chartprops.bheight)
            .attr("stroke", "none")
            .attr("fill", function(d) {return d3.rgb(chartprops.colorer(d.category));})
            .attr("transform", "translate(" + chartprops.margin + "," +
                  chartprops.margin + ")");

        // grid lines
        chartprops.svg.append("g")
            .attr("transform", "translate(" +
                  (chartprops.bpad + chartprops.margin) + "," +
                  (chartprops.h + chartprops.margin) + ")")
            .attr("class", "grid")
            .call(d3.svg.axis()
                  .scale(chartprops.scaler)
                  .tickSize(-chartprops.h, 0)
                  .tickFormat(""))
            .style("stroke", "#000")
            .style("opacity", 0.2);

        // ticks
        chartprops.svg.append("g")
            .attr("transform", "translate(" +
                  (chartprops.bpad + chartprops.margin) + "," +
                  (chartprops.h + chartprops.margin) + ")")
            .attr("class", "grid")
            .call(d3.svg.axis()
                  .scale(chartprops.scaler)
                  .orient('bottom')
                  .tickSize(0, 0)
                  .tickFormat(d3.format(".2f")));
    }

    function loadprofile (queryid, callback) {
        $.ajax({
            type: "GET",
            dataType: "json",
            url: "/profiles/" + queryid + ".json",
            success: callback,
            error: function (x, y, z) {
                console.log(x);
                console.log(y);
                console.log(z);
            }
        });
    }

    function setupglobalconfig (profile) {
        globalconfig.profile = profile;
        globalconfig.majorcolorscale = d3.scale.category20()
            .domain([0, d3.max(profile.fragmentProfile, accessor("majorFragmentId"))]);

    }

    String.prototype.endsWith = function(suffix) {
        return this.indexOf(suffix, this.length - suffix.length) !== -1;
    };

    loadprofile(globalconfig.queryid, function (profile) {
        setupglobalconfig(profile);

        var queryvisualdrawn = false;
        var timingoverviewdrawn = false;
        var jsonprofileshown = false;

        // trigger svg drawing when visible
        $('#query-tabs').on('shown.bs.tab', function (e) {
            if (queryvisualdrawn || !e.target.href.endsWith("#query-visual")) return;
            buildplangraph(d3.select("#query-visual-canvas"), profile.plan);
            queryvisualdrawn = true;
        })
        $('#fragment-accordion').on('shown.bs.collapse', function (e) {
            if (timingoverviewdrawn || e.target.id != "fragment-overview") return;
            buildtimingchart(d3.select("#fragment-overview-canvas"), extractminortimes(profile));
            timingoverviewdrawn = true;
        });

        // select default tabs
        $('#fragment-overview').collapse('show');
        $('#operator-overview').collapse('show');
        $('#query-tabs a[href="#query-query"]').tab('show');


        // add json profile on click
        $('#full-json-profile-json').on('shown.bs.collapse', function (e) {
            if (jsonprofileshown) return;
            $('#full-json-profile-json').html(JSON.stringify(globalconfig.profile, null, 4));
        });

        //builddomtable(d3.select("#timing-table")
        //            .append("tbody"), extractminortimes(profile),
        //            ["name", "start", "end"]);
    });
});
