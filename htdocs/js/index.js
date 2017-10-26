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

function main() {
    var tileLayer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
    });

    var _tl_0 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/AssignedLTS/MapServer/tile/{z}/{y}/{x}', {attribution:"Sean Lawrence"});
    var _tl_1 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_LTS12Islands/MapServer/tile/{z}/{y}/{x}', {attribution:"Sean Lawrence"});
    var _tl_2 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_PhilaShortestPath/MapServer/tile/{z}/{y}/{x}', {attribution:"Sean Lawrence"});
    var _tl_3 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/BSTRESS_SuburbanShortestPath/MapServer/tile/{z}/{y}/{x}', {attribution:"Sean Lawrence"});

    var layerControl = L.control.layers(null, {
            "A": _tl_0,
            "B": _tl_1,
            "C": _tl_2,
            "D": _tl_3
    });

    map = L.map('map', {
        center: [39.9522, -75.1639],
        zoom: 16,
        layers: [
            _tl_0,
            _tl_1,
            _tl_2,
            _tl_3
        ]
    }).addLayer(tileLayer);

    map.addControl(layerControl);

    printPlugin = L.easyPrint({
        title: 'Print',
        tileWait: 1000,
        position: 'topleft',
        sizeModes: [paperUSLetterP, paperUSLetterL]
    }).addTo(map);
}

main();