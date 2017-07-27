
const ICON_INIT = 'off';
const ICON_LOADING = 'refresh';
const ICON_OK = 'folder-open';
const ICON_ERROR = 'alert';

const state = {
    query: 'state:new',
    offset: 0,
    limit: 24 
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
        showConsole(selected.length + " items selected");
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
        preview.attr('title', image.tags.join(" "));
        preview.css('background-image', 'url("/api/thumb/' + image.key + '")');
        preview.data('key', image.key);
        
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

function showConsole(statusText) {
    setActionsEnabled(false);
    $('#console-status').html(statusText);
    $('#console').modal('show');
    $('#console-input').select();
}

function sendCommand(cmd) {
    var keys = [];
    $('DIV.selected').each(function(i) {
        keys.push($(this).data('key'));
    });

    var payload = {cmd, keys};

    setActionsEnabled(false);
    
    fetch('/api/cli', {
        method: 'POST',
        headers: new Headers({'Content-Type': 'application/json'}),
        body: JSON.stringify(payload)
    }).then((response) => {
        console.log(response);
        setActionsEnabled(true);
        $('#console').modal('hide');
        if (response.status == 200) {
            update()
        } else {
            alert(response.statusText);
        }
    })


}

function showDetail(image) {
    console.log(image.tags);
    $('#detail').modal('show');
    $('#detail-title').html(image.key);

    var el = $('#detail-image');
    el.hide()
        .attr('src', '/api/preview/' + image.key)
        .on('load', () => {
            console.log("FINISHED");
            el.slideDown();
        });

    var tags = $('<div/>');
    image.tags.map((tag) => {
        var badge = $('<span class="label label-primary"></span>').html(tag);
        tags.append(badge);
    });

    $('#detail-meta')
        .empty()
        .append(tags);

    fetch('/api/data/' + image.key).then((response) => {
        console.log(response);
        return response.text();
    }).then((data) => {
        $('#detail-meta').append($('<pre/>').html(data));
    });
}
