var ErlDocs = (function(index) {

    /* Search box height? */
    var RESULT_OFFSET_Y = 26;

    /* Human readable keyCode index */
    var KEY = {'BACKSPACE': 8, 'TAB': 9, 'NUM_PAD_CLEAR': 12, 'ENTER': 13, 'SHIFT': 16, 'CTRL': 17, 'ALT': 18, 'PAUSE': 19, 'CAPS_LOCK': 20, 'ESCAPE': 27, 'SPACEBAR': 32, 'PAGE_UP': 33, 'PAGE_DOWN': 34, 'END': 35, 'HOME': 36, 'ARROW_LEFT': 37, 'ARROW_UP': 38, 'ARROW_RIGHT': 39, 'ARROW_DOWN': 40, 'PRINT_SCREEN': 44, 'INSERT': 45, 'DELETE': 46, 'SEMICOLON': 59, 'WINDOWS_LEFT': 91, 'WINDOWS_RIGHT': 92, 'SELECT': 93, 'NUM_PAD_ASTERISK': 106, 'NUM_PAD_PLUS_SIGN': 107, 'NUM_PAD_HYPHEN-MINUS': 109, 'NUM_PAD_FULL_STOP': 110, 'NUM_PAD_SOLIDUS': 111, 'NUM_LOCK': 144, 'SCROLL_LOCK': 145, 'SEMICOLON': 186, 'EQUALS_SIGN': 187, 'COMMA': 188, 'HYPHEN-MINUS': 189, 'FULL_STOP': 190, 'SOLIDUS': 191, 'GRAVE_ACCENT': 192, 'LEFT_SQUARE_BRACKET': 219, 'REVERSE_SOLIDUS': 220, 'RIGHT_SQUARE_BRACKET': 221, 'APOSTROPHE': 222};

    (function () {
	    /* 0 - 9 */
	    for (var i = 48; i <= 57; i++) {
            KEY['' + (i - 48)] = i;
	    }
	    /* A - Z */
	    for (i = 65; i <= 90; i++) {
            KEY['' + String.fromCharCode(i)] = i;
	    }
	    /* NUM_PAD_0 - NUM_PAD_9 */
	    for (i = 96; i <= 105; i++) {
            KEY['NUM_PAD_' + (i - 96)] = i;
	    }
	    /* F1 - F12 */
	    for (i = 112; i <= 123; i++) {
            KEY['F' + (i - 112 + 1)] = i;
	    }
    })();

    var $search       = $("#search"),
        $results      = $("#results"),
        searchActive  = false,
        selected      = null,
        resultsCount  = 0,
        showingFuns   = true;


    var getDetails = function() {
        return showingFuns
            ? {"text":"Hide Functions", "cBottom": "25%", fTop: "75%"}
            : {"text":"View Functions", "cBottom": "0px", fTop: "100%"};
    };

    var setDetails = function(details, fun) {
        $("#viewfuns").text(details.text);
        $("#content").css({"bottom":details.cBottom});
        $("#funwrapper").animate({"top":details.fTop}, "normal", fun);
    };

    function scrollIntoView($parent, $child, force) {

        var childTop     = $child.position().top - RESULT_OFFSET_Y,
            scrollTop    = $parent[0].scrollTop,
            childHeight  = $child.height(),
            parentHeight = $parent.height(),
            newTop       = null;

	    if (force) {
	        $parent[0].scrollTop = scrollTop + childTop;
	        return;
	    }

        if (childTop < 0) {
            newTop = scrollTop + childTop;
        } else if (childTop + childHeight > parentHeight) {
            newTop = scrollTop + ((childTop + childHeight) - parentHeight);
        }

        if (newTop !== null) {
            if (Math.abs(newTop - scrollTop) > 200) {
                $parent.animate({"scrollTop": newTop}, 'fast');
            } else {
                $parent[0].scrollTop = newTop;
            }
        }
    };

    function setSelected(x, force) {

        var sel, children = $results.children("li");

        if (x >= 0 && x < resultsCount) {
	        if (selected !== null) {
	            children.eq(selected).removeClass("selected");
	        }
	        selected = x;
	        sel = children.eq(x).addClass("selected");
            if (sel.length > 0) {
                scrollIntoView($results, sel, force);
            }
        }
    };

    function keypress(e) {

        var tmp, blockedKeys = [0, KEY.TAB, KEY.CTRL, KEY.ALT,
				KEY.WINDOWS_LEFT, KEY.GRAVE_ACCENT];

	if (!($.inArray(e.keyCode, blockedKeys) === -1)) {
            return;
	}

        if        (e.keyCode === KEY.ARROW_DOWN) { setSelected(selected + 1);
        } else if (e.keyCode === KEY.ARROW_UP) { setSelected(selected - 1);
        } else if (e.keyCode === KEY.PAGE_DOWN) { setSelected(selected + 7);
        } else if (e.keyCode === KEY.PAGE_UP) { setSelected(selected - 7);
        } else if (e.keyCode === KEY.ENTER) {
            tmp = $results.children(".selected");
            if (tmp.length > 0) {
                document.location.href = tmp.find("a").attr("href");
            }
        } else {
	        filter($search.val());
        }
    };

    function windowResize() {
        $results.height($(window).height() - RESULT_OFFSET_Y);
    };

    function showModules() {

        var i, item, len  = index.length,
            results = [];

        for (i = 0; i < len; i += 1) {
            item = index[i];
            if (item[0] === "mod" || item[0] === "app") {
                results.push(item);
            }
        }
        return results;
    };

    function searchApps(str) {

        var i, count, item,
            len     = index.length,
            results = [],
            terms   = str.split(" ");

        for (i = 0, count = 0; i < len; i += 1) {
            item = index[i];
            if (match(item[2], terms)) {
                results.push(item);
                if (count++ > 100) {
                    break;
                }
            }
        }
        return results;
    };

    function formatResults(results, str) {

        var i, item, hash, url, path, html = "",
            len       = results.length,
            searchStr = isSearchStr(str) ? "&search="+str : "";

        for (i = 0; i < len; i += 1) {

            item = results[i];

	    if (item[0] === "app") {
		    hash = "#" + item[1];
		    path = "index";
	    } else {
		    hash = (item[0] === "fun") ? "#" + item[2].split(":")[1] : "";
		    path = item[1] + "/" + item[2].split(":")[0];
	    }
	    url = CURRENT_ROOT + path + ".html?i=" + i + searchStr + hash;

	    html += '<li class="' + item[0] + '"><a href="' + url + '">'
	        + '<span class="name">' + item[2] + "</span>"
	        + '<br /><span class="sub">' + item[3] + '</span>'
	        + '</a></li>';
        }

        return html;
    };

    function isSearchStr(str) {
        return typeof str !== "undefined" && str !== "";
    }

    function filter(str) {

        var results = isSearchStr(str) ? searchApps(str) : showModules(),
            html    = formatResults(results, str);

        $results[0].innerHTML = html;
        setSelected(0);
        resultsCount = results.length;
    };

    function match(str, terms) {
        for (var i = 0, len = terms.length; i < len; i += 1) {
	        if (str.match(new RegExp(terms[i], "i")) === null) {
	            return false;
	        }
        }
        return true;
    };

    function parseQuery(url) {

        var arr, query, i, len, tmp,
            qs = url.split("?")[1];

        if (typeof qs !== "undefined") {
	        arr   = qs.split("&");
            query = {};
	        for (i = 0, len = arr.length; i < len; i += 1) {
	            tmp = arr[i].split("=");
	            query[tmp[0]] = tmp[1];
	        }
	        return query;
        }
        return false;
    };

    function strToBool(val) {
        return val === "true";
    };

    function updateTitle() {
	var app, idx, mod, $t, i, j, results, len;
	results = showModules();
	len = results.length;
	$t = $("body h1"),
	mod = $t.text();
	for (i = 0; i < len; i++) {
	    idx = results[i];
	    if (idx[0] !== "mod" || idx[2] !== mod) continue;
	    app = idx[1];
	    for (j = i - 1; j >= 0; j--) {
		idx = results[j];
		if (idx[0] !== "app" || idx[1] !== app) continue;
		    $t.after(' (<a href="../index.html?i=' + j + '#' + app + '">' + app + '</a>)');
		return;
	    }
	}
    }

    function init() {

        var val, qs = parseQuery(document.location.search);

        if (qs && qs.search && $search.val() === "") {
            val = decodeURIComponent(qs.search.replace(/\+/g,  " "));
            $search.val(val);
            filter(val);

            // If a search parameter is specified and hash doesnt already
            // exist, jump to first match
            if (document.location.hash === "") {
                document.location.hash =
                    $("#results li:first-child a").attr("href").split("#")[1];
            }

        } else {
            filter();
        }

        if (qs && qs.i) {
            setSelected(parseInt(qs.i, 10), true);
        } else {
            setSelected(0);
        }

        if (qs && (qs.i || qs.search)) {
            $search[0].focus();
        } else {
            document.body.focus();
        }
    };

    $search.keydown(function (e) {

        // Run First keydown
        setTimeout(function () { keypress(e); }, 0);

        // // Set up a timer to repeat (holding down arrow keys etc)
        var timer = null, timeout = null,
            repeatKeys   = [KEY.PAGE_UP, KEY.PAGE_DOWN,
			    KEY.ARROW_UP, KEY.ARROW_DOWN,
			    KEY.BACKSPACE],
            set_interval = function () {
                timer = setInterval(function () { keypress(e); }, 60);
            };

        if ($.inArray(e.keyCode, repeatKeys) === -1) {
            return;
	    }

        function cleanup() {
            window.clearTimeout(timeout);
            $(document).unbind("keyup", cleanup);
            clearInterval(timer);
        }

        $search.bind("keyup", cleanup);
        timeout = window.setTimeout(set_interval, 300);
    });

    $search.focus(function () {
        $search[0].focus();
        $("#sidebar").removeClass("inactive");
        searchActive = true;
    });

    $search.blur(function () {
        $("#sidebar").addClass("inactive");
        searchActive = false;
        document.body.focus();
    });

    $(document).bind("keypress", function(e) {

        if (e.keyCode === KEY.TAB && !e.metaKey) {

            e.preventDefault();
            e.stopPropagation();

            if(searchActive) {
                $search.blur();
                $("#content")[0].focus();
            } else {
                $search[0].focus();
            }

            return false;
        }
        return true;
    });

    showingFuns = !!window.localStorage &&
        strToBool(window.localStorage.footer);

    if (document.title.match("Module Index") === null) {
	updateTitle();
	setDetails(getDetails(), function() {
		$("#funwrapper").css({"display":"block"});
	    });
    }

    $("#viewfuns").bind("mousedown", function(e) {

        showingFuns = !showingFuns;
        setDetails(getDetails(), null);

        if (!!window.localStorage) {
            window.localStorage.footer = showingFuns;
        }
    });

    $(window).bind('resize', windowResize);
    windowResize();

    init();

})(index);

/* Copyright (C) 2007, 2008 gnombat@users.sourceforge.net */
/* License: http://shjs.sourceforge.net/doc/gplv3.html */

if(!this.sh_languages){this.sh_languages={}}var sh_requests={};function sh_isEmailAddress(a){if(/^mailto:/.test(a)){return false}return a.indexOf("@")!==-1}function sh_setHref(b,c,d){var a=d.substring(b[c-2].pos,b[c-1].pos);if(a.length>=2&&a.charAt(0)==="<"&&a.charAt(a.length-1)===">"){a=a.substr(1,a.length-2)}if(sh_isEmailAddress(a)){a="mailto:"+a}b[c-2].node.href=a}function sh_konquerorExec(b){var a=[""];a.index=b.length;a.input=b;return a}function sh_highlightString(B,o){if(/Konqueror/.test(navigator.userAgent)){if(!o.konquered){for(var F=0;F<o.length;F++){for(var H=0;H<o[F].length;H++){var G=o[F][H][0];if(G.source==="$"){G.exec=sh_konquerorExec}}}o.konquered=true}}var N=document.createElement("a");var q=document.createElement("span");var A=[];var j=0;var n=[];var C=0;var k=null;var x=function(i,a){var p=i.length;if(p===0){return}if(!a){var Q=n.length;if(Q!==0){var r=n[Q-1];if(!r[3]){a=r[1]}}}if(k!==a){if(k){A[j++]={pos:C};if(k==="sh_url"){sh_setHref(A,j,B)}}if(a){var P;if(a==="sh_url"){P=N.cloneNode(false)}else{P=q.cloneNode(false)}P.className=a;A[j++]={node:P,pos:C}}}C+=p;k=a};var t=/\r\n|\r|\n/g;t.lastIndex=0;var d=B.length;while(C<d){var v=C;var l;var w;var h=t.exec(B);if(h===null){l=d;w=d}else{l=h.index;w=t.lastIndex}var g=B.substring(v,l);var M=[];for(;;){var I=C-v;var D;var y=n.length;if(y===0){D=0}else{D=n[y-1][2]}var O=o[D];var z=O.length;var m=M[D];if(!m){m=M[D]=[]}var E=null;var u=-1;for(var K=0;K<z;K++){var f;if(K<m.length&&(m[K]===null||I<=m[K].index)){f=m[K]}else{var c=O[K][0];c.lastIndex=I;f=c.exec(g);m[K]=f}if(f!==null&&(E===null||f.index<E.index)){E=f;u=K;if(f.index===I){break}}}if(E===null){x(g.substring(I),null);break}else{if(E.index>I){x(g.substring(I,E.index),null)}var e=O[u];var J=e[1];var b;if(J instanceof Array){for(var L=0;L<J.length;L++){b=E[L+1];x(b,J[L])}}else{b=E[0];x(b,J)}switch(e[2]){case -1:break;case -2:n.pop();break;case -3:n.length=0;break;default:n.push(e);break}}}if(k){A[j++]={pos:C};if(k==="sh_url"){sh_setHref(A,j,B)}k=null}C=w}return A}function sh_getClasses(d){var a=[];var b=d.className;if(b&&b.length>0){var e=b.split(" ");for(var c=0;c<e.length;c++){if(e[c].length>0){a.push(e[c])}}}return a}function sh_addClass(c,a){var d=sh_getClasses(c);for(var b=0;b<d.length;b++){if(a.toLowerCase()===d[b].toLowerCase()){return}}d.push(a);c.className=d.join(" ")}function sh_extractTagsFromNodeList(c,a){var f=c.length;for(var d=0;d<f;d++){var e=c.item(d);switch(e.nodeType){case 1:if(e.nodeName.toLowerCase()==="br"){var b;if(/MSIE/.test(navigator.userAgent)){b="\r"}else{b="\n"}a.text.push(b);a.pos++}else{a.tags.push({node:e.cloneNode(false),pos:a.pos});sh_extractTagsFromNodeList(e.childNodes,a);a.tags.push({pos:a.pos})}break;case 3:case 4:a.text.push(e.data);a.pos+=e.length;break}}}function sh_extractTags(c,b){var a={};a.text=[];a.tags=b;a.pos=0;sh_extractTagsFromNodeList(c.childNodes,a);return a.text.join("")}function sh_mergeTags(d,f){var a=d.length;if(a===0){return f}var c=f.length;if(c===0){return d}var i=[];var e=0;var b=0;while(e<a&&b<c){var h=d[e];var g=f[b];if(h.pos<=g.pos){i.push(h);e++}else{i.push(g);if(f[b+1].pos<=h.pos){b++;i.push(f[b]);b++}else{i.push({pos:h.pos});f[b]={node:g.node.cloneNode(false),pos:h.pos}}}}while(e<a){i.push(d[e]);e++}while(b<c){i.push(f[b]);b++}return i}function sh_insertTags(k,h){var g=document;var l=document.createDocumentFragment();var e=0;var d=k.length;var b=0;var j=h.length;var c=l;while(b<j||e<d){var i;var a;if(e<d){i=k[e];a=i.pos}else{a=j}if(a<=b){if(i.node){var f=i.node;c.appendChild(f);c=f}else{c=c.parentNode}e++}else{c.appendChild(g.createTextNode(h.substring(b,a)));b=a}}return l}function sh_highlightElement(d,g){sh_addClass(d,"sh_sourceCode");var c=[];var e=sh_extractTags(d,c);var f=sh_highlightString(e,g);var b=sh_mergeTags(c,f);var a=sh_insertTags(b,e);while(d.hasChildNodes()){d.removeChild(d.firstChild)}d.appendChild(a)}function sh_getXMLHttpRequest(){if(window.ActiveXObject){return new ActiveXObject("Msxml2.XMLHTTP")}else{if(window.XMLHttpRequest){return new XMLHttpRequest()}}throw"No XMLHttpRequest implementation available"}function sh_load(language,element,prefix,suffix){if(language in sh_requests){sh_requests[language].push(element);return}sh_requests[language]=[element];var request=sh_getXMLHttpRequest();var url=prefix+"sh_"+language+suffix;request.open("GET",url,true);request.onreadystatechange=function(){if(request.readyState===4){try{if(!request.status||request.status===200){eval(request.responseText);var elements=sh_requests[language];for(var i=0;i<elements.length;i++){sh_highlightElement(elements[i],sh_languages[language])}}else{throw"HTTP error: status "+request.status}}finally{request=null}}};request.send(null)}function sh_highlightDocument(g,k){var b=document.getElementsByTagName("pre");for(var e=0;e<b.length;e++){var f=b.item(e);var a=sh_getClasses(f);for(var c=0;c<a.length;c++){var h=a[c].toLowerCase();if(h==="sh_sourcecode"){continue}if(h.substr(0,3)==="sh_"){var d=h.substring(3);if(d in sh_languages){sh_highlightElement(f,sh_languages[d])}else{if(typeof(g)==="string"&&typeof(k)==="string"){sh_load(d,f,g,k)}else{throw'Found <pre> element with class="'+h+'", but no such language exists'}}break}}}};

sh_languages['erlang'] = [
  [
    [
      /\b(?:div|default|rem|or|xor|bor|bxor|bsl|bsr|and|band|not|bnot|abs|alive|apply|atom_to_list|binary_to_list|binary_to_term|concat_binary|date|disconnect_node|element|erase|exit|float|float_to_list|get|get_keys|group_leader|halt|hd|integer_to_list|is_alive|length|link|list_to_atom|list_to_binary|list_to_float|list_to_integer|list_to_pid|list_to_tuple|load_module|make_ref|monitor_node|node|nodes|now|open_port|pid_to_list|process_flag|process_info|process|put|register|registered|round|self|setelement|size|spawn|spawn_link|split_binary|statistics|term_to_binary|throw|time|tl|trunc|tuple_to_list|unlink|unregister|whereis|atom|binary|constant|function|integer|list|number|pid|ports|port_close|port_info|reference|record|check_process_code|delete_module|get_cookie|hash|math|module_loaded|preloaded|processes|purge_module|set_cookie|set_node|acos|asin|atan|atan2|cos|cosh|exp|log|log10|pi|pow|power|sin|sinh|sqrt|tan|tanh|call|module_info|parse_transform|undefined_function|error_handler|after|begin|case|catch|cond|end|fun|if|let|of|query|receive|when|creation|current_function|dictionary|group_leader|heap_size|high|initial_call|linked|low|memory_in_use|message_queue|net_kernel|node|normal|priority|reductions|registered_name|runnable|running|stack_trace|status|timer|trap_exit|waiting|command|count_in|count_out|creation|in|in_format|linked|node|out|owner|packeting|atom_tables|communicating|creation|current_gc|current_reductions|current_runtime|current_wall_clock|distribution_port|entry_points|error_handler|friends|garbage_collection|magic_cookie|magic_cookies|module_table|monitored_nodes|name|next_ref|ports|preloaded|processes|reductions|ref_state|registry|runtime|wall_clock|apply_lambda|module_info|module_lambdas|record|record_index|record_info|badarg|nocookie|false|fun|true|badsig|kill|killed|exit|normal)\b/g,
      'sh_keyword',
      -1
    ],
    [
      /%/g,
      'sh_comment',
      1
    ],
    [
      /\(|\)|\{|\}|\[|\]|\||;|,|\?|#/g,
      'sh_normal',
      -1
    ],
    [
      /\b[+-]?(?:(?:0x[A-Fa-f0-9]+)|(?:(?:[\d]*\.)?[\d]+(?:[eE][+-]?[\d]+)?))u?(?:(?:int(?:8|16|32|64))|L)?\b|\$\x+/g,
      'sh_number',
      -1
    ],
    [
      /"/g,
      'sh_string',
      2
    ],
    [
      /\'/g,
      'sh_string',
      3
    ],
    [
      /\w+(?:\s+)?[:@](?:\s+)?\w+/g,
      'sh_function',
      -1
    ],
    [
      /-compile|-define|-else|-endif|-export|-file|-ifdef|-ifndef|-import|-include|-include_lib|-module|-record|-undef|-author|-copyright|-doc/g,
      'sh_preproc',
      -1
    ],
    [
      /\+|-|\*|\/|==|=|=:=|=\/=|<|=<|>|>=|\+\+|--|=|!|<-|->|:|_|@|\\|\"|\./g,
      'sh_symbol',
      -1
    ]
  ],
  [
    [
      /$/g,
      null,
      -2
    ]
  ],
  [
    [
      /\\(?:\\|")/g,
      null,
      -1
    ],
    [
      /"/g,
      'sh_string',
      -2
    ]
  ],
  [
    [
      /\\(?:\\|\')/g,
      null,
      -1
    ],
    [
      /\'/g,
      'sh_string',
      -2
    ]
  ]
];
sh_highlightDocument();
