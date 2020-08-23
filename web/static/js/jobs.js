$(function () {

  /* Functions */

  var loadForm = function () {
    var btn = $(this);
    $.ajax({
      url: btn.attr("data-url"),
      type: 'get',
      dataType: 'json',
      beforeSend: function () {
        $("#modal-jobs .modal-content").html("");
        $("#modal-jobs").modal("show");
      },
      success: function (data) {
        $("#modal-jobs .modal-content").html(data.html_form);
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
          $("#jobs-table tbody").html(data.html_jobs_list);
          $("#modal-jobs").modal("hide");
        }
        else {
          $("#modal-jobs .modal-content").html(data.html_form);
        }
      }
    });
    return false;
  };


  /* Binding */

  // Add jobs
  $(".js-create-job").click(loadForm);
  $("#modal-jobs").on("submit", ".js-job-add-form", saveForm);

  // Describe jobs
  $("#jobs-table").on("click", ".js-describe-job", loadForm);
  $("#modal-jobs").on("submit", ".js-storage-update-form", saveForm);

  // Delete jobs
  $("#jobs-table").on("click", ".js-delete-job", loadForm);
  $("#modal-jobs").on("submit", ".js-job-delete-form", saveForm);

});
