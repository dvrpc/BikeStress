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

var Scout5 = {
    URL: "https://maps.gstatic.com/mapfiles/api-3/images/cb_scout5.png",
    dimensions: {w: 215, h: 835},
    // "Reverse Engineered" offset table
    offsets: [
        {offset:   0, min:   0.00, max:  11.25},
        {offset: -52, min:  11.25, max:  33.75},
        {offset:-104, min:  33.75, max:  56.25},
        {offset:-156, min:  56.25, max:  78.75},
        {offset:-208, min:  78.75, max: 101.25},
        {offset:-260, min: 101.25, max: 123.75},
        {offset:-312, min: 123.75, max: 146.25},
        {offset:-364, min: 146.25, max: 168.75},
        {offset:-416, min: 168.75, max: 191.25},
        {offset:-468, min: 191.25, max: 213.75},
        {offset:-520, min: 213.75, max: 236.25},
        {offset:-572, min: 236.25, max: 258.75},
        {offset:-624, min: 258.75, max: 281.25},
        {offset:-676, min: 281.25, max: 303.75},
        {offset:-728, min: 303.75, max: 326.25},
        {offset:-780, min: 326.25, max: 348.75},
        {offset:   0, min: 348.75, max: 360.00}
    ],

    headingToOffset: function(heading) {
        var success = false;
        var offset = null;
        for (var i in this.offsets) {
            var o = this.offsets[i];
            if ((o.min <= heading) && (heading < o.max)) {
                success = true;
                offset = o.offset;
                break;
            }
        }
        return {
            success: success,
            offset: offset
        };
    }
};

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
    layers: null,
    data: null,
    cameras: null
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
    for (var i in overlays.layers) {
        if (key_fn(overlays.layers[i])) {
            overlays.layers[i][key] = value;
            regenerateLayerControl();
            success = true;
            break;
        }
    }
    return success;
}

function _initOverlay() {
    overlays.layers = generateLayers();
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
        for (var i in overlays.layers) {
            if (overlays.layers[i].layer) {
                retval[overlays.layers[i].label] = overlays.layers[i].layer;
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
        var pos = panorama.getPosition();
        // console.log(pos.lat(), pos.lng());
    });
    panorama.addListener('pov_changed', function() {
        var pov = panorama.getPov();
        // console.log(pov.heading, pov.pitch);
        
        // $("#gmap").css("transform", "rotate(" + pov.heading + "deg)");
        // $("#gmap").css("-webkit-transform", "rotate(" + pov.heading + "deg)");
        // $("#gmap").css("-ms-transform", "rotate(" + pov.heading + "deg)");
        
        // $("#gpano").css("transform", "rotate(" + pov.pitch + "deg)");
        // $("#gpano").css("-webkit-transform", "rotate(" + pov.pitch + "deg)");
        // $("#gpano").css("-ms-transform", "rotate(" + pov.pitch + "deg)");
    });
    gmap.setStreetView(panorama);
    
    _partialClone(".gmnoprint>img", "#temp");
}

function _partialClone(src, trgt) {
    // var parent = $(src).parent()[0];
    var child = $(src)[0];
    // $(trgt).append(parent);
    // $(trgt + ">" + parent.tagName).append(child);
    $(trgt).append(child);
}

main();