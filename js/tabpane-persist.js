if (typeof Storage !== 'undefined') {
    const activeLanguage = localStorage.getItem('active_language');
    if (activeLanguage) {
        document
            .querySelectorAll('.persistLang-' + activeLanguage)
            .forEach((element) => {
              $('#' + element.id).tab('show');
            });
    }
}
function persistLang(language) {
    if (typeof Storage !== 'undefined') {
        localStorage.setItem('active_language', language);
        document.querySelectorAll('.persistLang-' + language)
          .forEach((element) => {
            $('#' + element.id).tab('show');
        });
    }
}
