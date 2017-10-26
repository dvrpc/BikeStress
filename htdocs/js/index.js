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

var overlays = 

[
    {
        group: "Guidance",
        items: [
            {
                label: "Regional Priorities - Philadelphia",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_RegionalPrioritiesPhila/MapServer/tile/{z}/{y}/{x}"
            },
            {
                label: "Regional Priorities - Suburban Counties",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_RegPrioritiesSuburban/MapServer/tile/{z}/{y}/{x}"
            }
        ]
    },
    {
        group: "Source",
        items: [
            {
                label: "Paved Shoulders",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_PavedShoulders/MapServer/tile/{z}/{y}/{x}"
            },
            {
                label: "Assigned LTS",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/AssignedLTS/MapServer/tile/{z}/{y}/{x}"
            }
        ]
    },
    {
        group: "Input",
        items: [
            {
                label: "LTS 1 and LTS 2 Islands",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_LTS12Islands/MapServer/tile/{z}/{y}/{x}"
            },
            {
                label: "LTS 3 - Philadelphia County",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_PhilaLTS3/MapServer/tile/{z}/{y}/{x}"
            },
            {
                label: "LTS 3 - Suburban Counties",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_SuburbanLTS123/MapServer/tile/{z}/{y}/{x}"
            }
        ]
    },
    {
        group: "Results",
        items: [
            {
                label: "Shortest Path - Philadelphia County",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_PhilaShortestPath/MapServer/tile/{z}/{y}/{x}"
            },
            {
                label: "Shortest Path - Suburban Counties",
                URL: "https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_SuburbanShortestPath/MapServer/tile/{z}/{y}/{x}"
            }
        ]
    },
];

function _formatOverlayLayerControl() {
    var flatLayers = [];
    for (var i0 in overlays) {
        var grp = overlays[i0];
        for (var i1 in grp.items) {
            var item = grp.items[i1];
            flatLayers[item.label] = item.URL;
        }
    }
    return flatLayers;
}

function main() {
    var tileLayer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
    });

    // var _tl_0 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/AssignedLTS/MapServer/tile/{z}/{y}/{x}', {attribution:"Sean Lawrence"});
    // var _tl_1 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_LTS12Islands/MapServer/tile/{z}/{y}/{x}', {attribution:"Sean Lawrence"});
    // var _tl_2 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_PhilaShortestPath/MapServer/tile/{z}/{y}/{x}', {attribution:"Sean Lawrence"});
    // var _tl_3 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_SuburbanShortestPath/MapServer/tile/{z}/{y}/{x}', {attribution:"Sean Lawrence"});

    // var layerControl = L.control.layers(null, {
            // "A": _tl_0,
            // "B": _tl_1,
            // "C": _tl_2,
            // "D": _tl_3
    // });
    _formatOverlayLayerControl();

    map = L.map('map', {
        center: [39.9522, -75.1639],
        zoom: 16,
        layers: [
            // _tl_0,
            // _tl_1,
            // _tl_2,
            // _tl_3
        ]
    }).addLayer(tileLayer);

    // map.addControl(layerControl);

    printPlugin = L.easyPrint({
        title: 'Print',
        tileWait: 1000,
        position: 'topleft',
        sizeModes: [paperUSLetterP, paperUSLetterL]
    }).addTo(map);
}

main();