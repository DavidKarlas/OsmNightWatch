var CallJosm = CallJosm || {};
CallJosm.Invoke = function (url) {
    var oReq = new XMLHttpRequest();
    oReq.open("get", url, true);
    oReq.send();
};