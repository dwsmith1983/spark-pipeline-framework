import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Spark Pipeline Framework',
  tagline: 'Configuration-driven Spark pipelines with HOCON and PureConfig',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  // GitHub Pages deployment
  url: 'https://dwsmith1983.github.io',
  baseUrl: '/spark-pipeline-framework/',
  organizationName: 'dwsmith1983',
  projectName: 'spark-pipeline-framework',
  trailingSlash: false,

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/dwsmith1983/spark-pipeline-framework/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/social-card.png',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Spark Pipeline Framework',
      logo: {
        alt: 'Spark Pipeline Framework Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'pathname:///spark-pipeline-framework/api/index.html',
          label: 'API',
          position: 'left',
        },
        {
          href: 'https://github.com/dwsmith1983/spark-pipeline-framework',
          label: 'GitHub',
          position: 'right',
        },
        {
          href: 'https://central.sonatype.com/namespace/io.github.dwsmith1983',
          label: 'Maven Central',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/getting-started',
            },
            {
              label: 'Configuration',
              to: '/docs/configuration',
            },
            {
              label: 'API Reference',
              href: 'pathname:///spark-pipeline-framework/api/index.html',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/dwsmith1983/spark-pipeline-framework',
            },
            {
              label: 'Maven Central',
              href: 'https://central.sonatype.com/namespace/io.github.dwsmith1983',
            },
            {
              label: 'Contributing',
              href: 'https://github.com/dwsmith1983/spark-pipeline-framework/blob/main/CONTRIBUTING.md',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Dustin Smith. Apache 2.0 License. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['java', 'bash', 'json', 'properties'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
