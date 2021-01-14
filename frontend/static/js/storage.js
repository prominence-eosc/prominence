$(function () {

  /* Functions */

  var loadForm = function () {
    var btn = $(this);
    $.ajax({
      url: btn.attr("data-url"),
      type: 'get',
      dataType: 'json',
      beforeSend: function () {
        $("#modal-storage .modal-content").html("");
        $("#modal-storage").modal("show");
      },
      success: function (data) {
        $("#modal-storage .modal-content").html(data.html_form);
      }
    });
  };

  var saveForm = function () {
    var form = $(this);
    $.ajax({
      url: form.attr("action"),
      data: form.serialize(),
      type: form.attr("method"),
      dataType: 'json',
      success: function (data) {
        if (data.form_is_valid) {
          $("#storage-table tbody").html(data.html_storage_list);
          $("#modal-storage").modal("hide");
        }
        else {
          $("#modal-storage .modal-content").html(data.html_form);
        }
      }
    });
    return false;
  };


  /* Binding */

  // Add storage
  $(".js-add-storage").click(loadForm);
  $("#modal-storage").on("submit", ".js-storage-add-form", saveForm);

  // Update storage
  $("#storage-table").on("click", ".js-update-storage", loadForm);
  $("#modal-storage").on("submit", ".js-storage-update-form", saveForm);

  // Delete storage
  $("#storage-table").on("click", ".js-delete-storage", loadForm);
  $("#modal-storage").on("submit", ".js-storage-delete-form", saveForm);

});
