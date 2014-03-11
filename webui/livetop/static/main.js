addMessage = function(text) {
    var div = $("<div>").addClass("msg").text(text);
    $("#target").prepend(div);
}
