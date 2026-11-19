// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://eventsalsa.dev',
	base: '/',
	scopedStyleStrategy: 'where',
	integrations: [
		starlight({
			title: 'eventsalsa',
			description: 'Website and documentation for the eventsalsa event sourcing bundle for Go.',
			lastUpdated: true,
			disable404Route: true,
			head: [
				{
					tag: 'meta',
					attrs: {
						name: 'go-import',
						content: 'eventsalsa.dev git https://github.com/eventsalsa/eventsalsa',
					},
				},
				{
					tag: 'meta',
					attrs: {
						name: 'go-source',
						content:
							'eventsalsa.dev https://github.com/eventsalsa/eventsalsa https://github.com/eventsalsa/eventsalsa/tree/main{/dir} https://github.com/eventsalsa/eventsalsa/blob/main{/dir}/{file}#L{line}',
					},
				},
			],
			customCss: ['/src/styles/design-theme.css'],
			components: {
				Header: './src/components/starlight/Header.astro',
			},
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/eventsalsa/eventsalsa' }],
			sidebar: [
				{
					label: 'Documentation',
					items: [
						{ label: 'Overview', slug: 'documentation' },
						{ label: 'Getting started', slug: 'documentation/getting-started' },
					],
				},
				{
					label: 'Components',
					items: [
						{ label: 'Store', slug: 'documentation/components/store' },
						{ label: 'Projector', slug: 'documentation/components/projector' },
						{ label: 'Encryption', slug: 'documentation/components/encryption' },
					],
				},
				{
					label: 'Project',
					items: [{ label: 'Changelog', slug: 'documentation/project/changelog' }],
				},
			],
		}),
	],
});
