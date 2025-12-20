import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/getting-started">
            Get Started
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            style={{marginLeft: '1rem'}}
            href="https://github.com/dwsmith1983/spark-pipeline-framework">
            GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

type FeatureItem = {
  title: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Configuration-Driven',
    description: (
      <>
        Define pipelines in HOCON config files. No code changes needed to
        add components, modify parameters, or reconfigure Spark settings.
      </>
    ),
  },
  {
    title: 'Type-Safe',
    description: (
      <>
        PureConfig provides compile-time type safety for your configuration.
        Catch errors early, not at runtime in production.
      </>
    ),
  },
  {
    title: 'Lifecycle Hooks',
    description: (
      <>
        Monitor execution, collect metrics, send alerts, and handle errors
        with composable lifecycle hooks.
      </>
    ),
  },
  {
    title: 'Cross-Platform',
    description: (
      <>
        Supports Spark 3.5.x and 4.0.x with Scala 2.12 and 2.13.
        Available on Maven Central.
      </>
    ),
  },
];

function Feature({title, description}: FeatureItem) {
  return (
    <div className={clsx('col col--3')}>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}

function QuickInstall(): ReactNode {
  return (
    <section style={{padding: '2rem 0', backgroundColor: 'var(--ifm-background-surface-color)'}}>
      <div className="container">
        <div className="text--center">
          <Heading as="h2">Quick Install</Heading>
          <pre style={{
            display: 'inline-block',
            textAlign: 'left',
            padding: '1rem 2rem',
            borderRadius: '8px',
            backgroundColor: 'var(--ifm-code-background)',
          }}>
            <code>
{`// build.sbt
libraryDependencies += "io.github.dwsmith1983" %% "spark-pipeline-runtime-spark3" % "<version>"`}
            </code>
          </pre>
          <p style={{marginTop: '1rem'}}>
            <Link href="https://central.sonatype.com/namespace/io.github.dwsmith1983">
              View on Maven Central →
            </Link>
          </p>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Home"
      description="Configuration-driven Spark pipelines with HOCON and PureConfig">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <QuickInstall />
      </main>
    </Layout>
  );
}
