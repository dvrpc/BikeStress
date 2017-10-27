var map,
    printPlugin;

var paperUSLetterP = {
    width: 850,
    height: 1100,
    className: 'A4Portrait page',
    name: 'Portrait'
}

var paperUSLetterL = {
    width: 1100,
    height: 840,
    className: 'A4Landscape page',
    name: 'Landscape'
}

var overlays = {
    definition: [
        {
            group: "Guidance",
            items: [
                {
                    label: "Regional Priorities - Philadelphia",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_RegionalPrioritiesPhila/MapServer"
                },
                {
                    label: "Regional Priorities - Suburban Counties",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_RegPrioritiesSuburban/MapServer"
                }
            ]
        },
        {
            group: "Source",
            items: [
                {
                    label: "Paved Shoulders",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_PavedShoulders/MapServer"
                },
                {
                    label: "Assigned LTS",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/AssignedLTS/MapServer"
                }
            ]
        },
        {
            group: "Input",
            items: [
                {
                    label: "LTS 1 and LTS 2 Islands",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_LTS12Islands/MapServer"
                },
                {
                    label: "LTS 3 - Philadelphia County",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_PhilaLTS3/MapServer"
                },
                {
                    label: "LTS 3 - Suburban Counties",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_SuburbanLTS123/MapServer"
                }
            ]
        },
        {
            group: "Results",
            items: [
                {
                    label: "Shortest Path - Philadelphia County",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_PhilaShortestPath/MapServer"
                },
                {
                    label: "Shortest Path - Suburban Counties",
                    URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_SuburbanShortestPath/MapServer"
                }
            ]
        },
    ],
    data: null
}

function _parseOverlay(base, args) {
    var agg_fn = function(a, v) { a[a.length] = v; return a };
    var xtr_fn = function(b) { return b; };
    var prc_fn = function(b, a) { return b; };
    if (typeof(args._process_fn) === "function") {
        prc_fn = args._process_fn;
    }
    if (typeof(args._retval_agg_fn) === "function") {
        agg_fn = args._retval_agg_fn;
    }
    if (typeof(args._base_extract_fn) === "function") {
        xtr_fn = args._base_extract_fn;
    }
    var retvals = [];
    var _base = xtr_fn(base);
    for (var i in _base) {
        retvals = agg_fn(retvals, prc_fn(_base[i], args));
    }
    return retvals;
}

function _parseOverlayItems(base, fn) {
    return _parseOverlay(base, {
        _process_fn: function(base, args) {
            return _parseOverlay(base, {
                _process_fn: function(base, args) {
                    return fn(base, args);
                },
                _base_extract_fn:  function(base) {
                    return base.items;
                }
            });
        },
        _retval_agg_fn: function(retvals, newval) {
            return retvals.concat(newval);
        }
    });
}

function _czechESRI(item) {
    _async_czechESRI(
        item.URL,
        _insertData,
        function(args) {
            return null;
        },
        {
            key_fn: function(_item) {
                return (_item.label === item.label);
            },
            key: "layer",
            value: _generateLayer(item)
        }
    );
}

function _async_czechESRI(URL, success, fail, args) {
    $.getJSON(URL + "/?f=json", function(json) {
        if (json.singleFusedMapCache) {
            success(args);
        } else {
            fail(args);
        }
    });
}

function _generateLayer(item) {
    return L.tileLayer(item.URL + "/tile/{z}/{y}/{x}", {
        attribution:"Sean Lawrence"
    });
}

function _insertData(args) {
    var success = false;
    for (var i in overlays.data) {
        if (args.key_fn(overlays.data[i])) {
            overlays.data[i][args.key] = args.value;
            regenerateLayerControl();
            success = true;
            break;
        }
    }
    return success;
}

function _initOverlay() {
    overlays.data = generateLayers();
    overlays.control = generateLayerControl();
    overlays.control.addTo(map);
}

function regenerateLayerControl() {
    map.removeControl(overlays.control);
    overlays.control = generateLayerControl();
    overlays.control.addTo(map);
}
function generateLayerControl() {
    return L.control.layers(null, (function() {
        var retval = {};
        for (var i in overlays.data) {
            if (overlays.data[i].layer) {
                retval[overlays.data[i].label] = overlays.data[i].layer;
            }
        }
        console.log(retval);
        return retval;
    })());
}

function generateLayers() {
    return _parseOverlayItems(overlays.definition, function(item, args) {
        _czechESRI(item);
        return {
            label: item.label,
            layer: null
        };
    });
}

function main() {
    var tileLayer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
    });

    map = L.map('map', {
        center: [39.9522, -75.1639],
        zoom: 16
    }).addLayer(tileLayer);

    printPlugin = L.easyPrint({
        title: 'Print',
        tileWait: 1000,
        position: 'topleft',
        sizeModes: [paperUSLetterP, paperUSLetterL]
    }).addTo(map);

    _initOverlay();
}

main();