window.onload = function () {
	var select = document.getElementsByName('resource_type')[0];
	select.dispatchEvent(new Event('change'));
}

$('select[name="resource_type"]').on('change',function(){
   var selectedVal=$(this).val();
   switch(selectedVal){
       case '1':
                   $('.ost_host').show();
                   $('.ost_username').show();
                   $('.ost_password').show();
                   $('.ost_tenant').show();
                   $('.ost_domain').show();
                   $('.ost_auth_version').show();
                   $('.ost_service_region').show();
                   $('.ost_tenant_domain_id').show();
                   $('.gcp_sa_email').hide();
                   $('.gcp_sa_private_key').hide();
                   $('.gcp_project').hide();
                   $('.gcp_regions').hide();
             break;
       case '2':
                   $('.ost_host').hide();
                   $('.ost_username').hide();
                   $('.ost_password').hide();
                   $('.ost_tenant').hide();
                   $('.ost_domain').hide();
                   $('.ost_auth_version').hide();
                   $('.ost_service_region').hide();
                   $('.ost_tenant_domain_id').hide();
                   $('.gcp_sa_email').show();
                   $('.gcp_sa_private_key').show();
                   $('.gcp_project').show();
                   $('.gcp_regions').show();

             break;
       default:
                   $('.token').hide();
                   $('.username').show();
                   $('.password').show();
             break;
   }
});

$(function () {

  /* Functions */

  var loadForm = function () {
    var btn = $(this);
    $.ajax({
      url: btn.attr("data-url"),
      type: 'get',
      dataType: 'json',
      beforeSend: function () {
        $("#modal-compute .modal-content").html("");
        $("#modal-compute").modal("show");
      },
      success: function (data) {
        $("#modal-compute .modal-content").html(data.html_form);
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
          $("#compute-table tbody").html(data.html_resources_list);
          $("#modal-compute").modal("hide");
        }
        else {
          $("#modal-compute .modal-content").html(data.html_form);
        }
      }
    });
    return false;
  };

  /* Binding */
  $("#compute-table").on("click", ".js-delete-compute", loadForm);
  $("#modal-compute").on("submit", ".js-compute-delete-form", saveForm);

});
