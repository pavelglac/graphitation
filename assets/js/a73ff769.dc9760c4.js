"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[433],{3905:function(e,t,r){r.d(t,{Zo:function(){return u},kt:function(){return d}});var a=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var c=a.createContext({}),p=function(e){var t=a.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=p(e.components);return a.createElement(c.Provider,{value:t},e.children)},s="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},f=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,o=e.originalType,c=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),s=p(r),f=n,d=s["".concat(c,".").concat(f)]||s[f]||m[f]||o;return r?a.createElement(d,i(i({ref:t},u),{},{components:r})):a.createElement(d,i({ref:t},u))}));function d(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=r.length,i=new Array(o);i[0]=f;var l={};for(var c in t)hasOwnProperty.call(t,c)&&(l[c]=t[c]);l.originalType=e,l[s]="string"==typeof e?e:n,i[1]=l;for(var p=2;p<o;p++)i[p]=r[p];return a.createElement.apply(null,i)}return a.createElement.apply(null,r)}f.displayName="MDXCreateElement"},1397:function(e,t,r){r.r(t),r.d(t,{assets:function(){return c},contentTitle:function(){return i},default:function(){return m},frontMatter:function(){return o},metadata:function(){return l},toc:function(){return p}});var a=r(3117),n=(r(7294),r(3905));const o={sidebar_position:3,id:"use-fragment",title:"useFragment",description:"API reference for useFragment, a React hook used to read fragment data from the Relay store using a fragment reference",keywords:["fragment","read","fragment reference"]},i=void 0,l={unversionedId:"packages/apollo-react-relay-duct-tape/use-fragment",id:"packages/apollo-react-relay-duct-tape/use-fragment",title:"useFragment",description:"API reference for useFragment, a React hook used to read fragment data from the Relay store using a fragment reference",source:"@site/docs/packages/apollo-react-relay-duct-tape/use-fragment.md",sourceDirName:"packages/apollo-react-relay-duct-tape",slug:"/packages/apollo-react-relay-duct-tape/use-fragment",permalink:"/graphitation/docs/packages/apollo-react-relay-duct-tape/use-fragment",draft:!1,editUrl:"https://github.com/microsoft/graphitation/tree/main/website/docs/packages/apollo-react-relay-duct-tape/use-fragment.md",tags:[],version:"current",sidebarPosition:3,frontMatter:{sidebar_position:3,id:"use-fragment",title:"useFragment",description:"API reference for useFragment, a React hook used to read fragment data from the Relay store using a fragment reference",keywords:["fragment","read","fragment reference"]},sidebar:"tutorialSidebar",previous:{title:"useLazyLoadQuery",permalink:"/graphitation/docs/packages/apollo-react-relay-duct-tape/use-lazy-load-query"},next:{title:"useRefetchableFragment",permalink:"/graphitation/docs/packages/apollo-react-relay-duct-tape/use-refetchable-fragment"}},c={},p=[{value:"<code>useFragment</code>",id:"usefragment",level:2},{value:"Arguments",id:"arguments",level:3},{value:"Return Value",id:"return-value",level:3},{value:"Behavior",id:"behavior",level:3}],u={toc:p},s="wrapper";function m(e){let{components:t,...r}=e;return(0,n.kt)(s,(0,a.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h2",{id:"usefragment"},(0,n.kt)("inlineCode",{parentName:"h2"},"useFragment")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-tsx"},'import React from "react";\nimport {\n  graphql,\n  useFragment,\n} from "@graphitation/apollo-react-relay-duct-tape";\n\nimport { UserComponent_user$key } from "./__generated__/UserComponent_user.graphql";\n\ninterface Props {\n  user: UserComponent_user$key;\n}\n\nconst UserComponent: React.FC<Props> = (props) => {\n  const data = useFragment(\n    graphql`\n      fragment UserComponent_user on User {\n        name\n        profile_picture(scale: 2) {\n          uri\n        }\n      }\n    `,\n    props.user\n  );\n\n  return (\n    <>\n      <h1>{data.name}</h1>\n      <div>\n        <img src={data.profile_picture?.uri} />\n      </div>\n    </>\n  );\n};\n\nexport default UserComponent;\n')),(0,n.kt)("h3",{id:"arguments"},"Arguments"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"fragment"),": GraphQL fragment specified using a ",(0,n.kt)("inlineCode",{parentName:"li"},"graphql")," template literal."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"fragmentReference"),": The ",(0,n.kt)("em",{parentName:"li"},"fragment reference")," is an opaque object that Apollo React/Relay Duct-Tape uses to read the data for the fragment from the store; more specifically, it contains information about which particular object instance the data should be read from.",(0,n.kt)("ul",{parentName:"li"},(0,n.kt)("li",{parentName:"ul"},"The type of the fragment reference can be imported from the generated TypeScript types, from the file ",(0,n.kt)("inlineCode",{parentName:"li"},"<fragment_name>.graphql.ts"),", and can be used to declare the type of your ",(0,n.kt)("inlineCode",{parentName:"li"},"Props"),". The name of the fragment reference type will be: ",(0,n.kt)("inlineCode",{parentName:"li"},"<fragment_name>$key"),".")))),(0,n.kt)("h3",{id:"return-value"},"Return Value"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"data"),": Object that contains data which has been read out from the store; the object matches the shape of specified fragment.",(0,n.kt)("ul",{parentName:"li"},(0,n.kt)("li",{parentName:"ul"},"The TypeScript type for data will also match this shape, and contain types derived from the GraphQL Schema. For example, the type of ",(0,n.kt)("inlineCode",{parentName:"li"},"data")," above is: ",(0,n.kt)("inlineCode",{parentName:"li"},"{ name: string, profile_picture: null | { uri: string } }"),".")))),(0,n.kt)("h3",{id:"behavior"},"Behavior"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The component is automatically subscribed to updates to the fragment data: if the data for this particular ",(0,n.kt)("inlineCode",{parentName:"li"},"User")," is updated anywhere in the app (e.g. via fetching new data, or mutating existing data), the component will automatically re-render with the latest updated data.")))}m.isMDXComponent=!0}}]);