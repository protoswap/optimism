export enum ChugSplashActionType {
  SET_CODE,
  SET_STORAGE,
}

export interface RawChugSplashAction {
  actionType: ChugSplashActionType
  target: string
  data: string
}

export interface SetCodeAction {
  target: string
  code: string
}

export interface SetStorageAction {
  target: string
  key: string
  value: string
}

export type ChugSplashAction = SetCodeAction | SetStorageAction

export interface ChugSplashActionBundle {
  root: string
  actions: Array<{
    action: RawChugSplashAction
    proof: {
      actionIndex: number
      siblings: string[]
    }
  }>
}
