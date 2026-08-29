import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://jigglyj2.github.io',
  base: '/StreamFusion',
  integrations: [
    starlight({
      title: 'StreamFusion',
      description: 'Accelerating Apache Flink SQL with Apache DataFusion.',
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/jigglyj2/StreamFusion' }],
      sidebar: [
        { label: 'Overview', items: [{ label: 'Introduction', slug: 'index' }, { label: 'Architecture', slug: 'architecture' }] },
        { label: 'Operators', autogenerate: { directory: 'operators' } },
        { label: 'Development', items: [{ label: 'Planning', slug: 'development/planning' }, { label: 'Metric compatibility', slug: 'development/metrics' }, { label: 'Memory and configuration', slug: 'development/memory-and-configuration' }, { label: 'Native modules and ABI', slug: 'development/native-modules' }, { label: 'SQL test harness', slug: 'development/sql-harness' }, { label: 'Planner integration', slug: 'development/planner-integration' }] },
        { label: 'Performance', items: [{ label: 'Nexmark benchmarks', slug: 'benchmarks/nexmark' }] }
      ]
    })
  ]
});
