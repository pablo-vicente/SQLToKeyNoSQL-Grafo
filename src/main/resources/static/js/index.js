// const baseUrl = "http://127.0.01:8080";
//
// getConnectores();
//
// async function getConnectores() {
//
//     const request = new Request(new URL(baseUrl + "/connectors"),
//         {
//             method: 'GET',
//         });
//
//     await fetch(request)
//         .then(async res => {
//             if (!res.ok) {
//                 await handleErro(res)
//                 return
//             };
//
//             const connectors = await res.json();
//             const select = document.querySelector("#select-target-nosql");
//
//             select.appendChild("<option value='' selected>Selecione o conector...</option>")
//
//             connectors.forEach(connector => {
//                 select.appendChild(`<option value="${connector}">${connector}</option>`)
//             });
//
//
//
//
//         })
// }
//
// async function handleErro(res) {
//     let response = await res.json();
//     console.log(response)
// }