/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./index.html", "./app.js"],
  theme: {
    extend: {
      colors: {
        "surface-container-low": "#f4f2fc", "surface": "#fbf8ff",
        "secondary": "#006a60", "primary": "#24389c",
        "surface-container-highest": "#e3e1ea", "surface-container": "#efedf6",
        "surface-container-lowest": "#ffffff", "error": "#ba1a1a",
        "on-surface-variant": "#454652", "primary-container": "#3f51b5",
        "primary-fixed": "#dee0ff", "secondary-container": "#85f6e5",
        "error-container": "#ffdad6", "on-error-container": "#93000a"
      },
      fontFamily: { "headline": ["Manrope", "sans-serif"], "body": ["Inter", "sans-serif"], "label": ["Inter", "sans-serif"] }
    },
  },
  plugins: [
    require('@tailwindcss/forms'),
    require('@tailwindcss/container-queries'),
  ],
}
