window.onload = function() {
    var anchors = document.getElementsByTagName("a");
        for (var i = 0; i < anchors.length; i++) {
            anchors[i].href = anchors[i].href.replace(/\.md$/,'\.html');
        }
    }