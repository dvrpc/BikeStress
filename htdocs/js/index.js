var map,
    gmap, // 2 maps is clearly better than 1
    panorama,
    printPlugin;

var options = {
    center: {
        lat: 39.9522,
        lng: -75.1639
    },
    zoom: 14
}

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

var ESRIBBoxLayer = L.TileLayer.extend({
    _will_bboxStr: function(bounds) {
        return ""
            + bounds.getWest()
            + "%2C" + bounds.getSouth()
            + "%2C" + bounds.getEast()
            + "%2C" + bounds.getNorth();
    },
    getTileUrl: function(t) {
        var bounds = this._tileCoordsToBounds(t);
        return this._url + "/export?"
            +       "dpi=" + "96"
            + "&" + "transparent=" + "true"
            + "&" + "format=" + "png32"
            + "&" + "layers=" + "show%3A0"
            + "&" + "bbox=" + this._will_bboxStr(bounds)
            + "&" + "bboxSR=" + "4326"
            + "&" + "imageSR=" + "102100"
            + "&" + "size=" + this.options.tileSize + "%2C" + this.options.tileSize
            + "&" + "f=" + "image";
    }
});

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
    var key_fn = function(_item) {
        return (_item.label === item.label);
    };
    var key = "layer";
    _async_czechESRI(
        item.URL,
        function() {
            _insertData(
                key_fn,
                key,
                _generateLayer(item)
            );
        },
        function() {
            _insertData(
                key_fn,
                key,
                _generateESRILayer(item)
            );
        }
    );
}

function _async_czechESRI(URL, success, fail) {
    $.getJSON(URL + "/?f=json", function(json) {
        if (json.singleFusedMapCache) {
            success();
        } else {
            fail();
        }
    });
}

function _generateLayer(item) {
    return L.tileLayer(item.URL + "/tile/{z}/{y}/{x}", {
        attribution: "Sean Lawrence"
    });
}

function _generateESRILayer(item) {
    return new ESRIBBoxLayer(item.URL, {
        tileSize: 512,
        attribution: "Lean Sawrence"
    });
}

function _insertData(key_fn, key, value) {
    var success = false;
    for (var i in overlays.data) {
        if (key_fn(overlays.data[i])) {
            overlays.data[i][key] = value;
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
        return retval;
    })(),{
        collapsed: false
    });
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

function _draggable_stop() {
}
function _resizable_stop() {
    google.maps.event.trigger(gmap, "resize");
    google.maps.event.trigger(panorama, "resize");
}

function main() {
    var tileLayer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
    });

    map = L.map('map', {
        center: options.center,
        zoom: options.zoom
    }).addLayer(tileLayer);

    printPlugin = L.easyPrint({
        title: 'Print',
        tileWait: 1000,
        position: 'topleft',
        sizeModes: [paperUSLetterP, paperUSLetterL]
    }).addTo(map);

    _initOverlay();

    map.on("click", function(e) {
        console.log(e);
        console.log(
            "https://maps.googleapis.com/maps/api/streetview?key=AIzaSyCPcULgKxlZHywXVke42fo1PVcd2So1GU8&size=540x200&heading=0&location="
            + e.latlng.lat + '%2C'
            + e.latlng.lng
        );
    });

    $(".draggable").draggable({
        containment: "#map",
        handle: ".handle",
        cancel: "#gmap",
        scroll: false,
        snap: true,
        grid: [25, 25],
        stop: function() {
            _draggable_stop();
        }
    });

    $(".resizable").resizable({
        containment: "#map",
        grid: 25,
        ghost: true,
        stop: function() {
            _resizable_stop();
        }
    });
}

function initialize() {
    gmap = new google.maps.Map(document.getElementById('gmap'), {
        center: options.center,
        zoom: options.zoom + 2
    });
    panorama = new google.maps.StreetViewPanorama(
        document.getElementById('gpano'), {
            position: options.center,
            pov: {
                heading: 34,
                pitch: 0
            }
        }
    );
    panorama.addListener('position_changed', function() {
        // console.log(panorama.getPosition());
    });
    gmap.setStreetView(panorama);
}

main();