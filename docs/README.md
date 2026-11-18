# eventsalsa docs

Website and documentation scaffold for **eventsalsa**, an event sourcing bundle for Go hosted at [`eventsalsa.dev`](https://eventsalsa.dev).

## Stack

- **Astro** for the website surface
- **Starlight** for documentation in Markdown/MDX
- **Cloudflare Pages** (with `functions/_middleware.ts` for zero-redirect `?go-get=1` vanity Go import resolution)

## Project structure

```text
.
├── functions/                  # Cloudflare Pages middleware (?go-get=1 Go vanity imports)
├── public/                     # Static assets
├── src/
│   ├── content/docs/           # Starlight documentation content
│   ├── layouts/                # Shared Astro layouts for the site
│   └── pages/                  # Custom website pages & static [...gopkg].astro routes
```

## Commands

```bash
npm install
npm run dev
npm run build
npm run preview
```

## Notes

- Product facts and documentation structure should come from the actual project and primary design sources, not from secondary exports.
