(function() {
    var bust = "?v=" + new Date().getTime();
    if (window.mikeVersionsUrl) {
        window.mikeVersionsUrl = window.mikeVersionsUrl.split('?')[0] + bust;
    } else {
        // Fallback if the variable hasn't been set yet
        window.mikeVersionsUrl = "/versions.json" + bust;
    }
})();
