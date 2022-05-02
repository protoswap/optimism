export type ConfigVariable =
  | boolean
  | string
  | number
  | Array<ConfigVariable>
  | {
      [name: string]: ConfigVariable
    }

export interface ChugSplashConfig {
  options: {
    name: string
    owner: string
  }
  contracts: {
    [name: string]: {
      source: string
      address?: string
      variables?: {
        [name: string]: ConfigVariable
      }
    }
  }
}

export interface CanonicalChugSplashConfig extends ChugSplashConfig {
  compiler: {
    langauge: 'solidity' // TODO: vyper support eventually
    version: string
    inputs: any[] // TODO: Properly type this
  }
}
