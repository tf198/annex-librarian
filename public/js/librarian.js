
const ICON_INIT = 'off';
const ICON_LOADING = 'refresh';
const ICON_OK = 'folder-open';
const ICON_ERROR = 'alert';

const state = {
    query: 'state:new',
    offset: 0,
    limit: 28,
}

var select_mode = false;

$(document).on('ready', () => {
    console.log("Document loaded");

    var searchInput = $('#search-input');
    
    searchInput.val(state.query);
    update();

    $('#search-form').on('submit', (e) => {
        Object.assign(state, {query: searchInput.val(), offset: 0});
        update(() => {
            searchInput.focus();
        });
        return false;
    });

    $('#state-next').on('click', (e) => {
        state.offset += state.limit;
        update();
    });

    $('#state-prev').on('click', (e) => {
        state.offset -= state.limit;
        if (state.offset < 0) state.offset = 0;
        update();
    });

    $('#action-select').on('click', (e) => {
        select_mode = (!select_mode);
        $(e.target).toggleClass('action-enabled', select_mode);
        if (!select_mode) {
            $('DIV.selected').removeClass('selected');
        }
    });

    $('#action-console').on('click', (e) => {
        var selected = $('DIV.selected');
        var message = (selected.length) ? "Metadata for " + selected.length + " items" : "No items selected";
        var prefix = (selected.length) ? "annex metadata" : "";

        showConsole(prefix, message);
    });

    $('#console-form').on("submit", (e) => {
        sendCommand($('#console-input').val());
    });

    $('#detail-image').on('click', (e) => {
        window.open(e.target.src, '_blank');
    });

    $('#detail-close').on('click', (e) => {
        $('#detail').modal('hide');
    });

    $('#console-close').on('click', (e) => {
        $('#console').modal('hide');
    });

    $('#detail-console').on('click', (e) => {
        var detail = $('#detail');
        var key = detail.data('key');
        detail.modal('hide');
        $('DIV.selected').removeClass('selected');
        $('#' + key).addClass('selected');
        
    });
});

function update(cb) {
    setStatus("Loading...", ICON_LOADING);
    var url = '/api/search?q=' + encodeURIComponent(state.query);
    if (state.offset) url += "&offset=" + state.offset;
    if (state.limit) url += "&limit=" + state.limit;

    setActionsEnabled(false);

    fetch(url).then((response) => {
        if (response.status != 200) {
            setStatus("Failed to execute query", ICON_ERROR);
            setActionsEnabled(true);
            return cb(new Error("Failed to execute search"));
        }

        return response.json();
    }).then((data) => {
        console.log(data);
        setStatus(data.total + " results", ICON_OK);
        $('#state-pos').html(state.offset + " to " + (state.offset+state.limit) + " of " + data.total);
        displayImages(data.matches);
        setActionsEnabled(true);
        cb && cb();
    });
}

function displayImages(images) {
    var imageGrid = $('<div/>');

    images.map((image) => {

        var preview = $('<div class="preview"/>');
        preview.attr('title', image.date || "unknown");
        preview.attr('id', image.key);
        preview.css('background-image', 'url("/api/thumb/' + image.key + '")');
        
        preview.on('click', (e) => {
            if (select_mode) {
                preview.toggleClass('selected');
            } else {
                showDetail(image);
            }
        });
        
        imageGrid.append(preview);
    });
    console.log(imageGrid);

    $('#image-grid').html(imageGrid);
}

function setStatus(text, icon) {
    console.log("Status: %s", text);
    $('#status-indicator').attr('class', 'glyphicon glyphicon-' + icon);
}

function setActionsEnabled(value) {
    $('.state-control').prop('disabled', !value);
}

function showConsole(prefix, statusText) {
    $('#console-status').html(statusText);
    $('#console-output').html('');
    $('#console-prefix').html(prefix);
    $('#console').modal('show');
    $('#console-input').select();
}

function sendCommand(cmd) {
    var keys = [];
    $('DIV.selected').each(function(i) {
        keys.push($(this).attr('id'));
    });

    var payload = {cmd}
    if (keys.length) {
        payload['keys'] = keys;
        payload['cmd'] = 'annex metadata ' + cmd;
    }
    console.log(payload);

    setActionsEnabled(false);
    
    fetch('/api/cli', {
        method: 'POST',
        headers: new Headers({'Content-Type': 'application/json'}),
        body: JSON.stringify(payload)
    }).then((response) => {
        setActionsEnabled(true);
        if (response.status == 200) {
            return response.json();
        } else {
            $('#console-output').html(response.statusText);
        }
    }).then((result) => {
        $('#console-output').html(result.message);
        if(result.result) {
            setTimeout(() => {
                $('#console').modal('hide');
            }, 1000);
        }
    });


}

function showDetail(image) {
    console.log(image);
    $('#detail').modal('show').data('key', image.key);
    $('#detail-title').html(image.key);

    var el = $('#detail-image');
    el.hide()
        .attr('src', '/api/preview/' + image.key)
        .on('load', () => {
            console.log("FINISHED");
            el.slideDown();
        });

    var tags = $('<div/>');

    $('#detail-meta')
        .empty()
        .append(tags);

    fetch('/api/data/' + image.key).then((response) => {
        console.log(response);
        return response.json();
    }).then((data) => {
        var meta = ""
        console.log(data)
        for (var s in data) {
            meta += "<strong>" + s + ":</strong>\n";
            if (typeof(data[s]) == 'object') {
                for (var k in data[s]) {
                    var v = data[s][k]
                    if (Array.isArray(v)) v = v.join(", ")
                    meta += "  <em>" + k + ":</em> " + v + "\n";
                }
            } else {
                meta += "  " + data[s] + "\n";
            }
        }

        $('#detail-meta').append($('<pre/>').html(meta));
        if (data['git-annex'] && data['git-annex']['tag']) {
            data['git-annex']['tag'].map((tag) => {
                var badge = $('<span class="label label-primary"></span>').html(tag);
                tags.append(badge);
            });
        } else {
            tags.html("<em>No tags</em>");
        }
    });
}
