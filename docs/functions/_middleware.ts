const GO_IMPORT_CONTENT = 'eventsalsa.dev git https://github.com/eventsalsa/eventsalsa';
const GO_SOURCE_CONTENT =
	'eventsalsa.dev https://github.com/eventsalsa/eventsalsa https://github.com/eventsalsa/eventsalsa/tree/main{/dir} https://github.com/eventsalsa/eventsalsa/blob/main{/dir}/{file}#L{line}';

interface PagesContext {
	request: Request;
	next: () => Promise<Response>;
}

function escapeHtml(value: string): string {
	return value
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/"/g, '&quot;')
		.replace(/'/g, '&#39;');
}

export async function onRequest(context: PagesContext): Promise<Response> {
	const { request } = context;

	if (request.method === 'GET' || request.method === 'HEAD') {
		const url = new URL(request.url);
		if (url.searchParams.get('go-get') === '1') {
			const cleanPath = url.pathname.replace(/\/+$/, '');
			const pkgPath = `eventsalsa.dev${cleanPath}`;
			const escapedPkg = escapeHtml(pkgPath);
			const pkgDocUrl = `https://pkg.go.dev/${escapedPkg}`;
			const body = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="go-import" content="${GO_IMPORT_CONTENT}" />
<meta name="go-source" content="${GO_SOURCE_CONTENT}" />
<meta http-equiv="refresh" content="0; url=${pkgDocUrl}" />
<title>${escapedPkg}</title>
</head>
<body>
Nothing to see here; <a href="${pkgDocUrl}">see the Go package documentation for ${escapedPkg}</a>.
</body>
</html>
`;

			return new Response(request.method === 'HEAD' ? null : body, {
				status: 200,
				headers: {
					'content-type': 'text/html; charset=utf-8',
					'cache-control': 'public, max-age=300',
				},
			});
		}
	}

	return context.next();
}
