$(function () {

  /* Functions */

  var loadForm = function () {
    var btn = $(this);
    $.ajax({
      url: btn.attr("data-url"),
      type: 'get',
      dataType: 'json',
      beforeSend: function () {
        $("#modal-workflows .modal-content").html("");
        $("#modal-workflows").modal("show");
      },
      success: function (data) {
        $("#modal-workflows .modal-content").html(data.html_form);
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
        $('#workflows-table').DataTable().ajax.reload();
        if (data.form_is_valid) {
          $("#workflows-table tbody").html(data.html_workflows_list);
          $("#modal-workflows").modal("hide");
        }
        else {
          $("#modal-workflows .modal-content").html(data.html_form);
        }
      }
    });
    return false;
  };


  /* Binding */

  // Add workflows
  $(".js-create-workflow").click(loadForm);
  $("#modal-workflows").on("submit", ".js-workflow-add-form", saveForm);

  // Describe workflows
  $("#workflows-table").on("click", ".js-describe-workflow", loadForm);
  $("#modal-workflows").on("submit", ".js-storage-update-form", saveForm);

  // Delete workflows:
  $("#workflows-table").on("click", ".js-delete-workflow", loadForm);
  $("#modal-workflows").on("submit", ".js-workflow-delete-form", saveForm);

});
