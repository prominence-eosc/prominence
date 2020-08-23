$('#revoke-token').on('show.bs.modal', function (event) {
    $(this).find('.modal-body').load(event.relatedTarget.href);
});
