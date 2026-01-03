```js
// ==UserScript==
// @name         Airflow Auto Login (simple + retry)
// @namespace    http://tampermonkey.net/
// @version      2026-01-03
// @match        *://*/*
// @grant        none
// ==/UserScript==

(function () {
    const USERNAME = "airflow"
    const PASSWORD = "airflow"

    const setNativeValue = (el, value) => {
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set
        setter.call(el, value)
        el.dispatchEvent(new Event("input", { bubbles: true }))
        el.dispatchEvent(new Event("change", { bubbles: true }))
    }

    let lastAttempt = 0
    let loggedIn = false

    setInterval(() => {
        if (loggedIn) return

        const form = document.querySelector('form[name="login"]')
        const userInput = document.querySelector("#username")
        const passInput = document.querySelector("#password")
        const submitBtn = document.querySelector('input[type="submit"]')

        if (!form || !userInput || !passInput || !submitBtn) {
            loggedIn = true
            return
        }

        const now = Date.now()
        if (now - lastAttempt < 2000) return
        lastAttempt = now

        if (userInput.value !== USERNAME) {
            userInput.focus()
            setNativeValue(userInput, USERNAME)
            userInput.blur()
        }

        if (passInput.value !== PASSWORD) {
            passInput.focus()
            setNativeValue(passInput, PASSWORD)
            passInput.blur()
        }

        if (!submitBtn.disabled) {
            submitBtn.click()
        }
    }, 200)
})()

```
